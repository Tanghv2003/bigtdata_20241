from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, current_timestamp
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType
import time
from datetime import datetime

class SparkKafkaConsumer:
    def __init__(self, app_name="FlightDataConsumer", kafka_topic="flights"):
        self.app_name = app_name
        self.kafka_topic = kafka_topic
        self.spark = None
        self.streaming_query = None
        self.user = "Tanghv2003"  # Current user
        self.start_time = datetime.utcnow()

    def create_spark_session(self):
        """Create and configure Spark session"""
        self.spark = (SparkSession.builder
                     .appName(self.app_name)
                     .master("spark://spark-master:7077")
                     .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0")
                     .config("spark.streaming.stopGracefullyOnShutdown", "true")
                     .config("spark.sql.shuffle.partitions", "2")
                     .getOrCreate())
        
        self.spark.sparkContext.setLogLevel("WARN")
        print(f"""
        ========================================
        Spark Session Created Successfully
        ----------------------------------------
        App Name: {self.app_name}
        User: {self.user}
        Start Time (UTC): {self.start_time}
        ========================================
        """)
        return self.spark

    def define_schema(self):
        """Define schema for the flight data"""
        return StructType([
            StructField("UniqueCarrier", StringType(), True),
            StructField("Origin", StringType(), True),
            StructField("Dest", StringType(), True),
            StructField("FlightNum", StringType(), True),
            StructField("Distance", IntegerType(), True),
            StructField("Timestamp", TimestampType(), True)
        ])

    def create_kafka_stream(self):
        """Create Kafka stream"""
        return (self.spark.readStream
                .format("kafka")
                .option("kafka.bootstrap.servers", "kafka:29092")
                .option("subscribe", self.kafka_topic)
                .option("startingOffsets", "latest")
                .option("kafka.security.protocol", "PLAINTEXT")
                .option("failOnDataLoss", "false")
                .load())

    def process_stream(self, kafka_df, schema):
        """Process streaming data"""
        return (kafka_df.selectExpr("CAST(value AS STRING) as json")
                .select(from_json("json", schema).alias("data"))
                .select("data.*"))

    def start_streaming(self):
        """Start the streaming process"""
        try:
            # Create Spark session
            if not self.spark:
                self.create_spark_session()

            # Define schema and create stream
            schema = self.define_schema()
            kafka_stream = self.create_kafka_stream()

            # Process stream
            processed_df = self.process_stream(kafka_stream, schema)

            # Start console output
            self.streaming_query = (processed_df.writeStream
                                  .format("console")
                                  .outputMode("append")
                                  .option("truncate", False)
                                  .trigger(processingTime="5 seconds")
                                  .start())

            print(f"""
            ========================================
            Streaming Started Successfully
            ----------------------------------------
            Topic: {self.kafka_topic}
            Current Time (UTC): {datetime.utcnow()}
            User: {self.user}
            ----------------------------------------
            Waiting for data...
            Press Ctrl+C to stop
            ========================================
            """)

            # Keep the application running
            self.streaming_query.awaitTermination()

        except Exception as e:
            print(f"Error in streaming: {str(e)}")
            raise
        finally:
            self.stop_streaming()

    def stop_streaming(self):
        """Stop streaming and clean up"""
        if self.streaming_query:
            try:
                self.streaming_query.stop()
                print("\nStreaming query stopped")
            except Exception as e:
                print(f"Error stopping query: {str(e)}")

        if self.spark:
            try:
                self.spark.stop()
                print("Spark session stopped")
            except Exception as e:
                print(f"Error stopping Spark: {str(e)}")

def main():
    consumer = SparkKafkaConsumer(
        app_name="FlightDataConsumer",
        kafka_topic="flights"
    )
    
    try:
        consumer.start_streaming()
    except KeyboardInterrupt:
        print("\nStreaming interrupted by user")
    except Exception as e:
        print(f"Error in main: {str(e)}")
    finally:
        consumer.stop_streaming()

if __name__ == "__main__":
    main()