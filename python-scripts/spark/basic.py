from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when
import pyspark.sql.functions as F
import pandas as pd

# Initialize Spark session
spark = SparkSession.builder \
    .appName("Airlines Data Analysis and Visualization") \
    .config("spark.es.nodes", "elasticsearch") \
    .config("spark.es.port", "9200") \
    .config("spark.es.index.auto.create", "true") \
    .config("spark.jars.packages", "org.elasticsearch:elasticsearch-spark-30_2.12:8.0.0") \
    .getOrCreate()

# HDFS file path
hdfs_file_path = "hdfs://namenode:8020/upload/data.csv"

# Read the data into a DataFrame
df = spark.read.option("header", "true").csv(hdfs_file_path)

# Data transformation: casting columns to appropriate data types
df = df.withColumn("ActualElapsedTime", col("ActualElapsedTime").cast("int")) \
    .withColumn("AirTime", col("AirTime").cast("int")) \
    .withColumn("ArrDelay", col("ArrDelay").cast("int")) \
    .withColumn("ArrTime", col("ArrTime").cast("int")) \
    .withColumn("CRSArrTime", col("CRSArrTime").cast("int")) \
    .withColumn("CRSDepTime", col("CRSDepTime").cast("int")) \
    .withColumn("CRSElapsedTime", col("CRSElapsedTime").cast("int")) \
    .withColumn("Cancelled", col("Cancelled").cast("int")) \
    .withColumn("DepDelay", col("DepDelay").cast("int")) \
    .withColumn("DepTime", col("DepTime").cast("int")) \
    .withColumn("Distance", col("Distance").cast("int")) \
    .withColumn("FlightNum", col("FlightNum").cast("int")) \
    .withColumn("TaxiIn", col("TaxiIn").cast("int")) \
    .withColumn("TaxiOut", col("TaxiOut").cast("int")) \
    .withColumn("Year", col("Year").cast("int"))



# Stop the Spark session
spark.stop()
