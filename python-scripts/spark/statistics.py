from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, month, avg, sum, countDistinct, when

# Initialize Spark session with MongoDB connector
spark = SparkSession.builder \
    .appName("Basic") \
    .config("spark.mongodb.write.connection.uri", "mongodb+srv://bigdata:bigdata2024@bigdata.axiis.mongodb.net/flight_stats?retryWrites=true&w=majority&appName=bigdata") \
    .config("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.12:3.0.2") \
    .config("spark.mongodb.output.maxBatchSize", "8192") \
    .master("spark://spark-master:7077") \
    .getOrCreate()

# Print Spark version for debugging
print(f"Spark version: {spark.version}")

# HDFS file path
hdfs_file_path = "hdfs://namenode:8020/upload/data.csv"

# Read the data into a DataFrame
df = spark.read.option("header", "true").csv(hdfs_file_path)

# Data transformation: casting columns to appropriate data types
df = df.withColumn("ArrDelay", col("ArrDelay").cast("int")) \
    .withColumn("DepDelay", col("DepDelay").cast("int")) \
    .withColumn("Year", col("Year").cast("int")) \
    .withColumn("Month", col("Month").cast("int")) \
    .withColumn("FlightNum", col("FlightNum").cast("int"))

# Function to write DataFrame to MongoDB
def write_to_mongodb(df, collection_name):
    write_uri = "mongodb+srv://bigdata:bigdata2024@bigdata.axiis.mongodb.net/flight_stats"
    df.write \
        .format("mongo") \
        .mode("overwrite") \
        .option("uri", write_uri) \
        .option("database", "flight_stats") \
        .option("collection", collection_name) \
        .save()

# Tổng số chuyến bay và hãng hàng không
summary_stats = spark.createDataFrame([
    (df.count(), df.select("UniqueCarrier").distinct().count())
], ["total_flights", "total_airlines"])
write_to_mongodb(summary_stats, "summary_stats")

# Số chuyến bay vào mỗi tháng
flights_per_month = df.groupBy("Month") \
    .agg(count("*").alias("TotalFlights")) \
    .orderBy("Month")
write_to_mongodb(flights_per_month, "flights_per_month")

# Thông tin delay về mỗi tháng
delay_info_per_month = df.groupBy("Month").agg(
    avg("ArrDelay").alias("AvgArrDelay"),
    avg("DepDelay").alias("AvgDepDelay"),
    sum(when(col("ArrDelay") > 0, 1).otherwise(0)).alias("TotalArrDelayFlights"),
    sum(when(col("DepDelay") > 0, 1).otherwise(0)).alias("TotalDepDelayFlights")
).orderBy("Month")
write_to_mongodb(delay_info_per_month, "delay_info_per_month")

# Số chuyến bay vào mỗi ngày trong tuần
flights_per_day = df.groupBy("DayOfWeek") \
    .agg(count("*").alias("TotalFlights")) \
    .orderBy("DayOfWeek")
write_to_mongodb(flights_per_day, "flights_per_day")

# Tỷ lệ hủy chuyến theo tháng
cancel_rate_per_month = df.groupBy("Month").agg(
    (sum(when(col("Cancelled") == 1, 1).otherwise(0)) / count("*") * 100).alias("CancelRate")
).orderBy("Month")
write_to_mongodb(cancel_rate_per_month, "cancel_rate_per_month")

# Thống kê theo hãng hàng không
airline_stats = df.groupBy("UniqueCarrier").agg(
    count("*").alias("TotalFlights"),
    avg("ArrDelay").alias("AvgArrDelay"),
    avg("DepDelay").alias("AvgDepDelay"),
    sum(when(col("Cancelled") == 1, 1).otherwise(0)).alias("CancelledFlights")
).orderBy("UniqueCarrier")
write_to_mongodb(airline_stats, "airline_stats")

# Thống kê theo sân bay
airport_stats = df.groupBy("Origin").agg(
    count("*").alias("TotalFlights"),
    avg("ArrDelay").alias("AvgArrDelay"),
    avg("DepDelay").alias("AvgDepDelay")
).orderBy("TotalFlights", ascending=False)
write_to_mongodb(airport_stats, "airport_stats")

spark.stop()