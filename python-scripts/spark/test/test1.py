from pyspark.sql import SparkSession

# Khởi tạo Spark session
spark = SparkSession.builder \
    .appName("Hadoop Text File Read Example") \
    .config("spark.hadoop.fs.defaultFS", "hdfs://namenode:8020") \
    .getOrCreate()

# Đọc dữ liệu từ file .txt trên HDFS
df = spark.read.text("hdfs://namenode:8020/test/hello.txt")

# Hiển thị nội dung dữ liệu
df.show(truncate=False)  # Dùng truncate=False để hiển thị toàn bộ dòng

# Hoặc nếu bạn chỉ muốn in ra một vài dòng đầu tiên
# df.show(5, truncate=False)
