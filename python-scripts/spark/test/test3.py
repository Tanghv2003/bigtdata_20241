from pyspark.sql import SparkSession

# Tạo một SparkSession kết nối đến Spark Master
spark = SparkSession.builder \
    .appName("Python Spark Job test") \
    .master("spark://spark-master:7077") \
    .getOrCreate()

# Ví dụ đơn giản: Tạo một DataFrame và thực hiện các phép toán
data = [("Alice", 1), ("Bob", 2), ("Catherine", 3)]
df = spark.createDataFrame(data, ["name", "value"])

# Hiển thị nội dung DataFrame
df.show()

# Đừng quên đóng SparkSession khi xong
spark.stop()
