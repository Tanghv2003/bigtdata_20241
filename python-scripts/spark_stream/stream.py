from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Initialize Spark session
spark = SparkSession.builder \
    .appName("Stream") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0") \
    .master("spark://spark-master:7077") \
    .getOrCreate()

# Read from Kafka   
kafka_stream = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:29092") \
    .option("subscribe", "flights") \
    .load()

# Extract message values as strings
messages = kafka_stream.selectExpr("CAST(value AS STRING)", "offset")

# Process and output messages
query = messages.writeStream \
    .outputMode("append") \
    .format("console") \
    .start()

# Wait for termination
query.awaitTermination()
