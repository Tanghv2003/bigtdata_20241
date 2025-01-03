from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when
from pyspark.ml import PipelineModel

# Tạo Spark session
spark = SparkSession.builder \
    .appName("Airlines Delay Prediction Test") \
    .master("spark://spark-master:7077") \
    .getOrCreate()

# Tạo sample DataFrame với schema giống dữ liệu gốc
sample_data = [
    (1, 1, 1, 2023, 300.0, 200.0, 15.0, "AA", "JFK", "LAX", ""),
    (2, 2, 2, 2023, 250.0, 300.0, 30.0, "UA", "LAX", "SFO", ""),
    (3, 3, 3, 2023, 180.0, 100.0, 5.0, "DL", "SFO", "SEA", "")
]

schema = ["Month", "DayofMonth", "DayOfWeek", "Year", 
          "CRSElapsedTime", "Distance", "DepDelay", 
          "UniqueCarrier", "Origin", "Dest", "Diverted"]

sample_df = spark.createDataFrame(sample_data, schema=schema)



# Load model từ HDFS
model_path = "hdfs://namenode:8020/models/airline_delay_prediction"
loaded_model = PipelineModel.load(model_path)
print("Đã load model thành công")

# Dự đoán với sample DataFrame
predictions = loaded_model.transform(sample_df)

# Hiển thị kết quả dự đoán
print("\nKết quả dự đoán:")
predictions.select("Month", "DayofMonth", "DayOfWeek", "Year",
                   "CRSElapsedTime", "Distance", "DepDelay",
                   "UniqueCarrier", "Origin", "Dest", "Diverted",
                   "prediction", "probability").show()

# Stop Spark session
spark.stop()