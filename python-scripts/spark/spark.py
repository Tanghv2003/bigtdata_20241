from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, count

# Tạo SparkSession và cấu hình kết nối với Elasticsearch
spark = SparkSession.builder \
    .appName("Read from HDFS and Send to Elasticsearch") \
    .config("spark.es.nodes", "elasticsearch") \
    .config("spark.es.port", "9200") \
    .config("spark.es.index.auto.create", "true") \
    .config("spark.jars.packages", "org.elasticsearch:elasticsearch-spark-30_2.12:8.0.0") \
    .getOrCreate()

# Đọc dữ liệu từ file CSV trên HDFS
hdfs_file_path = "hdfs://namenode:8020/tanghv/test.csv"

# Đọc dữ liệu từ HDFS với định dạng CSV và có header
df = spark.read.option("header", "true").csv(hdfs_file_path)

# Chuyển đổi các cột kiểu dữ liệu cần thiết (chẳng hạn như Cancelled và Month)
df_processed = df.withColumn("Cancelled", col("Cancelled").cast("int")) \
    .withColumn("Month", col("Month").cast("int"))

# Tính số chuyến bay thành công và bị hủy mỗi tháng
flight_status_counts = df_processed.groupBy("Month") \
    .agg(
        count(when(col("Cancelled") == 0, 1)).alias("Successful_Flights"),
        count(when(col("Cancelled") == 1, 1)).alias("Cancelled_Flights")
    )

# Hiển thị kết quả để kiểm tra
flight_status_counts.show(truncate=False)

# Gửi kết quả lên Elasticsearch
flight_status_counts.write \
    .format("org.elasticsearch.spark.sql") \
    .option("es.resource", "flights_monthly_status/_doc") \
    .mode("overwrite") \
    .save()


print("Gửi thành công\n")
# Dừng Spark session
spark.stop()
