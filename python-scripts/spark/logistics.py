from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, hour, minute, rand
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
from pyspark.ml.feature import StringIndexer, OneHotEncoder, VectorAssembler, StandardScaler
from pyspark.ml import Pipeline
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.evaluation import MulticlassClassificationEvaluator

# Tạo Spark session
spark = SparkSession.builder \
    .appName("Flight Delay Prediction") \
    .master("spark://spark-master:7077") \
    .getOrCreate()

# Định nghĩa schema cho dữ liệu
schema = StructType([
    StructField("UniqueCarrier", StringType(), True),
    StructField("Origin", StringType(), True),
    StructField("Dest", StringType(), True),
    StructField("Month", IntegerType(), True),
    StructField("DayofMonth", IntegerType(), True),
    StructField("FlightNum", StringType(), True),
    StructField("CRSDepTime", StringType(), True),
    StructField("DepDelay", DoubleType(), True),
    StructField("Distance", DoubleType(), True),
    StructField("CRSArrTime", StringType(), True),
    StructField("Diverted", IntegerType(), True),
    StructField("Cancelled", IntegerType(), True),
    StructField("RouteType", StringType(), True)
])

try:
    # Đọc dữ liệu
    df = spark.read.schema(schema).csv("hdfs://namenode:8020/upload/2009.csv")

    # Tạo features từ thời gian
    df = df.withColumn("DepHour", hour(col("CRSDepTime"))) \
           .withColumn("DepMinute", minute(col("CRSDepTime"))) \
           .withColumn("ArrHour", hour(col("CRSArrTime"))) \
           .withColumn("ArrMinute", minute(col("CRSArrTime")))

    # Tạo nhãn delay dựa trên DepDelay
    df = df.withColumn("DelayCategory",
        when(col("DepDelay").isNull() | (col("DepDelay") < 15), 0)  # Đúng giờ hoặc sớm
        .when((col("DepDelay") >= 15) & (col("DepDelay") < 45), 1)  # Delay nhẹ
        .when((col("DepDelay") >= 45) & (col("DepDelay") < 90), 2)  # Delay trung bình
        .when(col("DepDelay") >= 90, 3)  # Delay nặng
        .otherwise(0)
    )

    # Định nghĩa features
    categorical_features = ["UniqueCarrier", "Origin", "Dest", "RouteType"]
    numeric_features = ["Month", "DayofMonth", "Distance", "DepHour", "DepMinute", 
                       "ArrHour", "ArrMinute", "Diverted"]

    # Xử lý missing values
    df = df.na.fill(0, numeric_features)
    df = df.na.fill("UNKNOWN", categorical_features)

    # Cân bằng dữ liệu bằng oversampling
    print("\nPhân phối dữ liệu trước khi cân bằng:")
    df.groupBy("DelayCategory").count().orderBy("DelayCategory").show()

    # Tách dữ liệu theo category
    df_0 = df.filter(col("DelayCategory") == 0)
    df_1 = df.filter(col("DelayCategory") == 1)
    df_2 = df.filter(col("DelayCategory") == 2)
    df_3 = df.filter(col("DelayCategory") == 3)

    # Đếm số lượng mẫu của mỗi category
    count_0 = df_0.count()
    count_1 = df_1.count()
    count_2 = df_2.count()
    count_3 = df_3.count()
    max_count = max(count_0, count_1, count_2, count_3)

    # Oversample các category có ít mẫu hơn
    df_1_oversampled = df_1.sample(True, float(max_count)/float(count_1), seed=42) if count_1 > 0 else df_1
    df_2_oversampled = df_2.sample(True, float(max_count)/float(count_2), seed=42) if count_2 > 0 else df_2
    df_3_oversampled = df_3.sample(True, float(max_count)/float(count_3), seed=42) if count_3 > 0 else df_3

    # Kết hợp dữ liệu đã được cân bằng
    balanced_df = df_0.unionAll(df_1_oversampled)\
                     .unionAll(df_2_oversampled)\
                     .unionAll(df_3_oversampled)

    print("\nPhân phối dữ liệu sau khi cân bằng:")
    balanced_df.groupBy("DelayCategory").count().orderBy("DelayCategory").show()

    # Tạo pipeline stages cho categorical features
    indexers = [StringIndexer(inputCol=feat, outputCol=f"{feat}Index", handleInvalid="keep") 
               for feat in categorical_features]
    encoders = [OneHotEncoder(inputCol=f"{feat}Index", outputCol=f"{feat}Vec", handleInvalid="keep") 
               for feat in categorical_features]

    # Vector Assembler
    assembler = VectorAssembler(
        inputCols=numeric_features + [f"{feat}Vec" for feat in categorical_features],
        outputCol="raw_features",
        handleInvalid="keep"
    )

    # Scaler
    scaler = StandardScaler(
        inputCol="raw_features",
        outputCol="features",
        withStd=True,
        withMean=False
    )

    # Logistic Regression với các tham số được điều chỉnh
    lr = LogisticRegression(
        featuresCol="features",
        labelCol="DelayCategory",
        maxIter=50,  # Tăng số lần lặp
        family="multinomial",
        elasticNetParam=0.3,
        regParam=0.01  # Thêm regularization
    )

    # Tạo và train pipeline
    pipeline = Pipeline(stages=indexers + encoders + [assembler, scaler, lr])
    
    # Split data
    train_data, test_data = balanced_df.randomSplit([0.8, 0.2], seed=42)
    print("\nSố lượng dữ liệu train:", train_data.count())
    print("Số lượng dữ liệu test:", test_data.count())
    
    # Train model
    print("\nBắt đầu training model...")
    model = pipeline.fit(train_data)
    print("Hoàn thành training model")
    
    # Lưu model
    model_path = "hdfs://namenode:8020/models/flight_delay_prediction2"
    model.write().overwrite().save(model_path)
    print(f"\nĐã lưu model tại: {model_path}")
    
    # Đánh giá model
    predictions = model.transform(test_data)
    
    # Tính các metrics
    evaluator_accuracy = MulticlassClassificationEvaluator(
        labelCol="DelayCategory",
        predictionCol="prediction",
        metricName="accuracy"
    )
    
    evaluator_f1 = MulticlassClassificationEvaluator(
        labelCol="DelayCategory",
        predictionCol="prediction",
        metricName="f1"
    )
    
    accuracy = evaluator_accuracy.evaluate(predictions)
    f1_score = evaluator_f1.evaluate(predictions)
    
    print(f"\nKết quả đánh giá model:")
    print(f"Accuracy: {accuracy:.4f}")
    print(f"F1 Score: {f1_score:.4f}")
    
    # Hiển thị confusion matrix
    print("\nConfusion Matrix:")
    predictions.groupBy("DelayCategory", "prediction") \
              .count() \
              .orderBy("DelayCategory", "prediction") \
              .show()

    # Lưu metadata
    model_metadata = {
        "metrics": {
            "accuracy": float(accuracy),
            "f1_score": float(f1_score)
        },
        "features": {
            "categorical": categorical_features,
            "numeric": numeric_features
        },
        "training_date": spark.sql("SELECT current_timestamp()").collect()[0][0].strftime("%Y-%m-%d %H:%M:%S")
    }
    
    # Lưu metadata
    import json
    metadata_path = "hdfs://namenode:8020/models/flight_delay_prediction_metadata.json"
    with open("/tmp/metadata.json", "w") as f:
        json.dump(model_metadata, f)
    
    from subprocess import call
    call(["hdfs", "dfs", "-put", "-f", "/tmp/metadata.json", metadata_path])
    print(f"\nĐã lưu metadata tại: {metadata_path}")

except Exception as e:
    print("\nLỗi trong quá trình training hoặc lưu model:")
    print(str(e))
    
finally:
    spark.stop()