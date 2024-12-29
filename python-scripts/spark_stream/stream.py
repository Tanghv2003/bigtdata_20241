from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, expr
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
import time

def create_spark_session():
    """Create Spark session with necessary configurations"""
    return (SparkSession.builder
            .appName("FlightDataConsumer")
            .master("spark://spark-master:7077")
            # Add configurations for Kafka
            .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0")
            .config("spark.streaming.stopGracefullyOnShutdown", "true")
            .config("spark.sql.shuffle.partitions", "2")
            .getOrCreate())

def define_schema():
    """Define schema for flight data"""
    return StructType([
        StructField("UniqueCarrier", StringType(), True),
        StructField("Origin", StringType(), True),
        StructField("Dest", StringType(), True),
        StructField("Month", IntegerType(), True),
        StructField("DayofMonth", IntegerType(), True),
        StructField("FlightNum", StringType(), True),
        StructField("CRSDepTime", StringType(), True),
        StructField("Distance", IntegerType(), True),
        StructField("CRSArrTime", StringType(), True),
        StructField("Diverted", IntegerType(), True),
        StructField("Cancelled", IntegerType(), True),
        StructField("RouteType", StringType(), True)
    ])

def create_kafka_stream(spark):
    """Create and return Kafka stream"""
    return (spark.readStream
            .format("kafka")
            .option("kafka.bootstrap.servers", "kafka:29092")
            .option("subscribe", "flights")
            .option("startingOffsets", "latest")
            .option("kafka.security.protocol", "PLAINTEXT")
            .option("failOnDataLoss", "false")
            .load())

def process_stream(df, schema):
    """Process the streaming data"""
    return (df.selectExpr("CAST(value AS STRING) as json")
            .select(from_json("json", schema).alias("data"))
            .select("data.*"))

def start_streaming(processed_df):
    """Start the streaming query with console output"""
    return (processed_df.writeStream
            .format("console")
            .outputMode("append")
            .option("truncate", False)
            .option("numRows", 10)
            .trigger(processingTime="5 seconds")
            .start())

def main():
    spark = None
    query = None
    
    try:
        # Create Spark Session
        print("Initializing Spark Session...")
        spark = create_spark_session()
        spark.sparkContext.setLogLevel("WARN")
        print("Spark Session created successfully!")

        # Define schema
        print("Defining schema...")
        schema = define_schema()

        # Create Kafka stream
        print("Creating Kafka stream...")
        kafka_stream = create_kafka_stream(spark)
        
        # Process stream
        print("Processing stream...")
        processed_df = process_stream(kafka_stream, schema)

        # Add debug information
        print("Stream Schema:")
        processed_df.printSchema()

        # Start streaming
        print("Starting streaming query...")
        query = start_streaming(processed_df)
        
        print("""
        ====================================
        Streaming is active and running!
        Waiting for data from Kafka topic 'flights'...
        Press Ctrl+C to stop the streaming
        ====================================
        """)

        # Monitor the query
        while query.isActive:
            print(f"Active: {query.isActive}, Recent progress: {len(query.recentProgress)}")
            if len(query.recentProgress) > 0:
                print(f"Latest batch: {query.recentProgress[-1]}")
            time.sleep(5)

    except Exception as e:
        print(f"Error occurred: {str(e)}")
        import traceback
        print(traceback.format_exc())

    finally:
        # Clean up
        if query:
            try:
                query.stop()
                print("Streaming query stopped")
            except Exception as e:
                print(f"Error stopping query: {str(e)}")

        if spark:
            try:
                spark.stop()
                print("Spark Session stopped")
            except Exception as e:
                print(f"Error stopping Spark: {str(e)}")

if __name__ == "__main__":
    main()