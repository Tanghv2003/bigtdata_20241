from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, hour, minute , when
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
from pyspark.ml import PipelineModel
from datetime import datetime

class FlightDelayPredictor:
    def __init__(self, app_name="FlightDelayPredictor", kafka_topic="flights"):
        self.app_name = app_name
        self.kafka_topic = kafka_topic
        self.spark = None
        self.streaming_query = None
        self.user = "Tanghv2003"
        self.start_time = datetime.utcnow()

    def create_spark_session(self):
        """Tạo và cấu hình Spark session"""
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
        """Định nghĩa schema cho dữ liệu flight"""
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

    def create_kafka_stream(self):
        """Tạo Kafka stream"""
        return (self.spark.readStream
                .format("kafka")
                .option("kafka.bootstrap.servers", "kafka:29092")
                .option("subscribe", self.kafka_topic)
                .option("startingOffsets", "latest")
                .option("kafka.security.protocol", "PLAINTEXT")
                .option("failOnDataLoss", "false")
                .load())

    def preprocess_data(self, df):
        """Tiền xử lý dữ liệu trước khi dự đoán"""
        # Tạo features từ thời gian
        return df.withColumn("DepHour", hour(col("CRSDepTime"))) \
                .withColumn("DepMinute", minute(col("CRSDepTime"))) \
                .withColumn("ArrHour", hour(col("CRSArrTime"))) \
                .withColumn("ArrMinute", minute(col("CRSArrTime"))) \
                .na.fill(0, ["Diverted", "DepHour", "DepMinute", "ArrHour", "ArrMinute", "Distance"]) \
                .na.fill("UNKNOWN", ["UniqueCarrier", "Origin", "Dest", "RouteType"])

    def process_stream(self, kafka_df, schema, model):
        """Xử lý streaming data và thực hiện dự đoán"""
        # Parse JSON data
        parsed_df = (kafka_df.selectExpr("CAST(value AS STRING) as json")
                    .select(from_json("json", schema).alias("data"))
                    .select("data.*"))
        
        # Tiền xử lý dữ liệu
        processed_df = self.preprocess_data(parsed_df)
        
        # Thực hiện dự đoán
        predictions = model.transform(processed_df)
        
        # Chọn các cột cần thiết và thêm mô tả delay
        result_df = predictions.select(
            "UniqueCarrier", 
            "Origin", 
            "Dest", 
            "CRSDepTime", 
            "Distance",
            "prediction"
        ).withColumn(
            "DelayDescription",
            when(col("prediction") == 0, "Đúng giờ hoặc sớm")
            .when(col("prediction") == 1, "Delay nhẹ (15-45 phút)")
            .when(col("prediction") == 2, "Delay trung bình (45-90 phút)")
            .when(col("prediction") == 3, "Delay nặng (>90 phút)")
        )
        
        return result_df

    def start_streaming(self):
        """Khởi động quá trình streaming"""
        try:
            # Tạo Spark session
            if not self.spark:
                self.create_spark_session()

            print(f"""
            ========================================
            Starting Flight Delay Prediction Stream
            ----------------------------------------
            Current Date and Time (UTC): {datetime.utcnow()}
            Current User's Login: {self.user}
            ========================================
            """)

            # Định nghĩa schema và tạo stream
            schema = self.define_schema()
            kafka_stream = self.create_kafka_stream()

            # Load model đã được train
            model_path = "hdfs://namenode:8020/models/flight_delay_prediction"
            model = PipelineModel.load(model_path)

            # Xử lý stream và thực hiện dự đoán
            predictions_df = self.process_stream(kafka_stream, schema, model)

            # Hiển thị kết quả
            self.streaming_query = (predictions_df.writeStream
                                  .format("console")
                                  .outputMode("append")
                                  .option("truncate", False)
                                  .trigger(processingTime="5 seconds")
                                  .start())

            print(f"""
            ========================================
            Flight Delay Prediction Stream Started
            ----------------------------------------
            Topic: {self.kafka_topic}
            Current Time (UTC): {datetime.utcnow()}
            User: {self.user}
            ----------------------------------------
            Waiting for flight data...
            Press Ctrl+C to stop
            ========================================
            """)

            self.streaming_query.awaitTermination()

        except Exception as e:
            print(f"Error in streaming: {str(e)}")
            raise
        finally:
            self.stop_streaming()

    def stop_streaming(self):
        """Dừng streaming và dọn dẹp tài nguyên"""
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
    predictor = FlightDelayPredictor(
        app_name="FlightDelayPredictor",
        kafka_topic="flights"
    )
    
    try:
        predictor.start_streaming()
    except KeyboardInterrupt:
        print("\nStreaming interrupted by user")
    except Exception as e:
        print(f"Error in main: {str(e)}")
    finally:
        predictor.stop_streaming()

if __name__ == "__main__":
    main()