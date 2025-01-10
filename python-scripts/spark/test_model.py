from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
from pyspark.sql.functions import col, hour, minute
from pyspark.ml import PipelineModel

# Tạo Spark session
spark = SparkSession.builder \
    .appName("Flight Delay Prediction Test") \
    .master("spark://spark-master:7077") \
    .getOrCreate()

# Định nghĩa schema giống như dữ liệu training
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

# Tạo dữ liệu mẫu
sample_data = [
    # Chuyến bay 1: Khả năng cao đúng giờ
    ("AA", "LAX", "SFO", 1, 10, "AA123", "0800", 0.0, 337.0, "0930", 0, 0, "S"),
    
    # Chuyến bay 2: Khả năng delay nhẹ
    ("UA", "ORD", "JFK", 1, 10, "UA456", "1200", 20.0, 740.0, "1500", 0, 0, "M"),
    
    # Chuyến bay 3: Khả năng delay trung bình
    ("DL", "ATL", "MIA", 1, 10, "DL789", "1600", 60.0, 594.0, "1800", 0, 0, "M"),
    
    # Chuyến bay 4: Khả năng delay nặng
    ("WN", "DEN", "LAS", 1, 10, "WN101", "2000", 100.0, 629.0, "2200", 0, 0, "S"),
    
    # Chuyến bay 5: Chuyến bay sáng sớm
    ("AS", "SEA", "PDX", 1, 10, "AS202", "0600", 5.0, 129.0, "0700", 0, 0, "S")
]

try:
    # Tạo DataFrame từ dữ liệu mẫu
    test_df = spark.createDataFrame(sample_data, schema)
    
    # Tạo features từ thời gian
    test_df = test_df.withColumn("DepHour", hour(col("CRSDepTime"))) \
                    .withColumn("DepMinute", minute(col("CRSDepTime"))) \
                    .withColumn("ArrHour", hour(col("CRSArrTime"))) \
                    .withColumn("ArrMinute", minute(col("CRSArrTime")))
    
    # Load model đã lưu
    model_path = "hdfs://namenode:8020/models/flight_delay_prediction"
    loaded_model = PipelineModel.load(model_path)
    
    # Thực hiện dự đoán
    predictions = loaded_model.transform(test_df)
    
    # Hiển thị kết quả
    print("\nKết quả dự đoán:")
    results = predictions.select(
        "UniqueCarrier", 
        "Origin", 
        "Dest", 
        "CRSDepTime",
        "Distance",
        "DepDelay",
        "prediction"
    )
    
    # Thêm mô tả cho prediction
    from pyspark.sql.functions import when
    results = results.withColumn(
        "DelayDescription",
        when(col("prediction") == 0, "Đúng giờ hoặc sớm")
        .when(col("prediction") == 1, "Delay nhẹ (15-45 phút)")
        .when(col("prediction") == 2, "Delay trung bình (45-90 phút)")
        .when(col("prediction") == 3, "Delay nặng (>90 phút)")
    )
    
    # Hiển thị kết quả
    results.show(truncate=False)
    
    # Tính toán xác suất cho mỗi category
    print("\nXác suất dự đoán cho từng category:")
    probability_results = predictions.select(
        "UniqueCarrier",
        "Origin",
        "Dest",
        "CRSDepTime",
        "probability"
    )
    probability_results.show(truncate=False)
    
except Exception as e:
    print("\nLỗi trong quá trình test model:")
    print(str(e))
    
finally:
    spark.stop()