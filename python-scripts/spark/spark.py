from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, count, sum
import pyspark.sql.functions as F


spark = SparkSession.builder \
    .appName("Read from HDFS and Send to Elasticsearch") \
    .config("spark.es.nodes", "elasticsearch") \
    .config("spark.es.port", "9200") \
    .config("spark.es.index.auto.create", "true") \
    .config("spark.jars.packages", "org.elasticsearch:elasticsearch-spark-30_2.12:8.0.0") \
    .getOrCreate()

hdfs_file_path = "hdfs://namenode:8020//hadoop/uploads/airline.csv_chunk_0"

df = spark.read.option("header", "true").csv(hdfs_file_path)


df_processed = df.withColumn("Cancelled", F.col("Cancelled").cast("int")) \
    .withColumn("Month", F.col("Month").cast("int")) \
    .withColumn("ActualElapsedTime", F.col("ActualElapsedTime").cast("int")) \
    .withColumn("AirTime", F.col("AirTime").cast("int")) \
    .withColumn("ArrDelay", F.col("ArrDelay").cast("int")) \
    .withColumn("ArrTime", F.col("ArrTime").cast("int")) \
    .withColumn("CRSArrTime", F.col("CRSArrTime").cast("int")) \
    .withColumn("CRSDepTime", F.col("CRSDepTime").cast("int")) \
    .withColumn("CRSElapsedTime", F.col("CRSElapsedTime").cast("int")) \
    .withColumn("CarrierDelay", F.col("CarrierDelay").cast("int")) \
    .withColumn("DepDelay", F.col("DepDelay").cast("int")) \
    .withColumn("DepTime", F.col("DepTime").cast("int")) \
    .withColumn("Dest", F.col("Dest")) \
    .withColumn("Distance", F.col("Distance").cast("int")) \
    .withColumn("Diverted", F.col("Diverted").cast("int")) \
    .withColumn("FlightNum", F.col("FlightNum")) \
    .withColumn("LateAircraftDelay", F.col("LateAircraftDelay").cast("int")) \
    .withColumn("NASDelay", F.col("NASDelay").cast("int")) \
    .withColumn("Origin", F.col("Origin")) \
    .withColumn("SecurityDelay", F.col("SecurityDelay").cast("int")) \
    .withColumn("TailNum", F.col("TailNum")) \
    .withColumn("TaxiIn", F.col("TaxiIn").cast("int")) \
    .withColumn("TaxiOut", F.col("TaxiOut").cast("int")) \
    .withColumn("UniqueCarrier", F.col("UniqueCarrier")) \
    .withColumn("WeatherDelay", F.col("WeatherDelay").cast("int")) \
    .withColumn("Year", F.col("Year").cast("int"))


flight_status_counts = df_processed.groupBy("Month") \
    .agg(
        F.count(F.when(F.col("Cancelled") == 0, 1)).alias("Successful_Flights"),
        F.count(F.when(F.col("Cancelled") == 1, 1)).alias("Cancelled_Flights"),
        
        F.sum(F.when(F.col("ArrDelay").isNotNull(), F.col("ArrDelay")).otherwise(0)).alias("Total_ArrDelay"),
        F.sum(F.when(F.col("DepDelay").isNotNull(), F.col("DepDelay")).otherwise(0)).alias("Total_DepDelay"),
        
        F.sum(F.when(F.col("CarrierDelay").isNotNull(), F.col("CarrierDelay")).otherwise(0)).alias("Total_CarrierDelay"),
        F.sum(F.when(F.col("NASDelay").isNotNull(), F.col("NASDelay")).otherwise(0)).alias("Total_NASDelay"),
        F.sum(F.when(F.col("WeatherDelay").isNotNull(), F.col("WeatherDelay")).otherwise(0)).alias("Total_WeatherDelay"),
        F.sum(F.when(F.col("LateAircraftDelay").isNotNull(), F.col("LateAircraftDelay")).otherwise(0)).alias("Total_LateAircraftDelay"),
        F.sum(F.when(F.col("SecurityDelay").isNotNull(), F.col("SecurityDelay")).otherwise(0)).alias("Total_SecurityDelay"),
        F.sum(F.when(F.col("TaxiIn").isNotNull(), F.col("TaxiIn")).otherwise(0)).alias("Total_TaxiIn"),
        F.sum(F.when(F.col("TaxiOut").isNotNull(), F.col("TaxiOut")).otherwise(0)).alias("Total_TaxiOut")
    )


flight_status_counts.show()

flight_status_counts.write \
    .format("org.elasticsearch.spark.sql") \
    .option("es.resource", "flights_monthly_status/_doc") \
    .mode("overwrite") \
    .save()




cancelled_by_carrier = df_processed.groupBy("UniqueCarrier") \
    .agg(
        count(when(col("Cancelled") == 1, 1)).alias("Cancelled_Flights")
    )


cancelled_by_carrier.show()


cancelled_by_carrier.write \
    .format("org.elasticsearch.spark.sql") \
    .option("es.resource", "flights_by_carrier/_doc") \
    .mode("overwrite") \
    .save()



print("Gửi thành công\n")

# Dừng Spark session
spark.stop()
