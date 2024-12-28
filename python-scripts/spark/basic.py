from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, month, avg, sum, countDistinct, when

# Initialize Spark session
spark = SparkSession.builder \
    .appName("Basic") \
    .config("spark.es.port", "9200") \
    .config("spark.es.index.auto.create", "true") \
    .config("spark.jars.packages", "graphframes:graphframes:0.8.2-spark3.0-s_2.12") \
    .master("spark://spark-master:7077") \
    .getOrCreate()

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

# Tổng số chuyến bay
total_flights = df.count()
print(f"Tổng số chuyến bay: {total_flights}")

# Tổng số hãng hàng không
total_airlines = df.select("UniqueCarrier").distinct().count()
print(f"Tổng số hãng hàng không: {total_airlines}")

# Số chuyến bay vào mỗi tháng
flights_per_month = df.groupBy("Month").agg(count("*").alias("TotalFlights")).orderBy("Month")
#flights_per_month.write.format("es").option("es.resource", "flights_per_month").save()

# Thông tin delay về mỗi tháng
delay_info_per_month = df.groupBy("Month").agg(
    avg("ArrDelay").alias("AvgArrDelay"),
    avg("DepDelay").alias("AvgDepDelay"),
    sum(when(col("ArrDelay") > 0, 1).otherwise(0)).alias("TotalArrDelayFlights"),
    sum(when(col("DepDelay") > 0, 1).otherwise(0)).alias("TotalDepDelayFlights")
).orderBy("Month")
delay_info_per_month.show()
# delay_info_per_month.write.format("es").option("es.resource", "delay_info_per_month").save()

# Số chuyến bay vào mỗi ngày trong tuần
flights_per_day = df.groupBy("DayOfWeek").agg(count("*").alias("TotalFlights")).orderBy("DayOfWeek")
flights_per_day.show()
# flights_per_day.write.format("es").option("es.resource", "flights_per_day").save()

# Tỷ lệ hủy chuyến theo tháng
cancel_rate_per_month = df.groupBy("Month").agg(
    (sum(when(col("Cancelled") == 1, 1).otherwise(0)) / count("*") * 100).alias("CancelRate")
).orderBy("Month")
cancel_rate_per_month.show()
# cancel_rate_per_month.write.format("es").option("es.resource", "cancel_rate_per_month").save()

# Thống kê theo hãng hàng không
airline_stats = df.groupBy("UniqueCarrier").agg(
    count("*").alias("TotalFlights"),
    avg("ArrDelay").alias("AvgArrDelay"),
    avg("DepDelay").alias("AvgDepDelay"),
    sum(when(col("Cancelled") == 1, 1).otherwise(0)).alias("CancelledFlights")
).orderBy("UniqueCarrier")
airline_stats.show()
# airline_stats.write.format("es").option("es.resource", "airline_stats").save()

# Thống kê theo sân bay (Origin)
airport_stats = df.groupBy("Origin").agg(
    count("*").alias("TotalFlights"),
    avg("ArrDelay").alias("AvgArrDelay"),
    avg("DepDelay").alias("AvgDepDelay")
).orderBy("TotalFlights", ascending=False)
airport_stats.show()
# airport_stats.write.format("es").option("es.resource", "airport_stats").save()

spark.stop()
