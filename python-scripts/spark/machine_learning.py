from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when
from pyspark.ml.feature import StringIndexer, OneHotEncoder, VectorAssembler, StandardScaler, Imputer
from pyspark.ml import Pipeline
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.evaluation import MulticlassClassificationEvaluator

# Tạo Spark session
spark = SparkSession.builder \
    .appName("Airlines Delay Prediction") \
    .master("spark://spark-master:7077") \
    .getOrCreate()

# Load và kiểm tra dữ liệu
df = spark.read.option("header", "true").csv("hdfs://namenode:8020/upload/data.csv")
print("Số lượng dòng ban đầu:", df.count())
print("Schema ban đầu:")
df.printSchema()

# Xử lý dữ liệu null
df = df.na.drop(subset=["ArrDelay"])
print("Số lượng dòng sau khi xóa null:", df.count())

# Chuyển đổi kiểu dữ liệu cho các cột số
numeric_columns = ["ArrDelay", "Month", "DayofMonth", "DayOfWeek", "Year", 
                  "CRSElapsedTime", "Distance", "DepDelay"]
for col_name in numeric_columns:
    df = df.withColumn(col_name, col(col_name).cast("double"))

# Tạo nhãn delay
df = df.withColumn("DelayCategory",
    when(col("ArrDelay") < 15, 0)
    .when((col("ArrDelay") >= 15) & (col("ArrDelay") < 45), 1)
    .when((col("ArrDelay") >= 45) & (col("ArrDelay") < 90), 2)
    .when((col("ArrDelay") >= 90) & (col("ArrDelay") < 150), 3)
    .when((col("ArrDelay") >= 150) & (col("ArrDelay") < 240), 4)
    .otherwise(5)
)

# Kiểm tra phân phối của nhãn
print("\nPhân phối của DelayCategory:")
df.groupBy("DelayCategory").count().orderBy("DelayCategory").show()

# Định nghĩa features
categorical_features = ["UniqueCarrier", "Origin", "Dest", "Diverted"]
numeric_features = ["Month", "DayofMonth", "DayOfWeek", "Year", 
                   "CRSElapsedTime", "Distance", "DepDelay"]

# Kiểm tra và xử lý null trong features
df = df.na.fill(0, numeric_features)  # Fill 0 cho numeric features
df = df.na.fill("UNKNOWN", categorical_features)  # Fill "UNKNOWN" cho categorical features

# Tạo pipeline stages
indexers = [StringIndexer(inputCol=feat, outputCol=f"{feat}Index", handleInvalid="keep") 
           for feat in categorical_features]
encoders = [OneHotEncoder(inputCol=f"{feat}Index", outputCol=f"{feat}Vec", handleInvalid="keep") 
           for feat in categorical_features]

# Imputer cho numeric features
imputer = Imputer(
    inputCols=numeric_features,
    outputCols=numeric_features,
    strategy="mean"
)

# Vector Assembler với error handling
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

# Logistic Regression
lr = LogisticRegression(
    featuresCol="features",
    labelCol="DelayCategory",
    maxIter=10,
    family="multinomial"
)

# Tạo và train pipeline
try:
    pipeline = Pipeline(stages=indexers + encoders + [imputer, assembler, scaler, lr])
    
    # Split data
    train_data, test_data = df.randomSplit([0.8, 0.2], seed=42)
    print("\nSố lượng dữ liệu train:", train_data.count())
    print("Số lượng dữ liệu test:", test_data.count())
    
    # Fit model
    print("\nBắt đầu training model...")
    model = pipeline.fit(train_data)
    print("Hoàn thành training model")
    
    # Lưu model lên HDFS
    model_path = "hdfs://namenode:8020/models/airline_delay_prediction"
    model.write().overwrite().save(model_path)
    print(f"\nĐã lưu model tại: {model_path}")
    
    # Load model từ HDFS để kiểm tra
    from pyspark.ml import PipelineModel
    loaded_model = PipelineModel.load(model_path)
    print("Đã load model thành công")
    
    # Predictions với loaded model
    predictions = loaded_model.transform(test_data)
    
    # Evaluate
    evaluator = MulticlassClassificationEvaluator(
        labelCol="DelayCategory",
        predictionCol="prediction",
        metricName="accuracy"
    )
    accuracy = evaluator.evaluate(predictions)
    print(f"\nĐộ chính xác của model: {accuracy}")
    
    # Show sample predictions
    print("\nMẫu dự đoán:")
    predictions.select("DelayCategory", "prediction", "probability").show(20)

    # Lưu metadata của model
    model_metadata = {
        "accuracy": accuracy,
        "features": {
            "categorical": categorical_features,
            "numeric": numeric_features
        },
        "training_date": spark.sql("SELECT current_timestamp()").collect()[0][0]
    }
    
    # Lưu metadata dưới dạng JSON
    import json
    metadata_path = "hdfs://namenode:8020/models/airline_delay_prediction_metadata.json"
    with open("/tmp/metadata.json", "w") as f:
        json.dump(model_metadata, f)
    
    # Copy file từ local lên HDFS
    from subprocess import call
    call(["hdfs", "dfs", "-put", "-f", "/tmp/metadata.json", metadata_path])
    print(f"\nĐã lưu metadata tại: {metadata_path}")

except Exception as e:
    print("\nLỗi trong quá trình training hoặc lưu model:")
    print(str(e))
    
finally:
    # Stop Spark session
    spark.stop()