from pyspark.sql import SparkSession

# Khởi tạo SparkSession kết nối đến Spark cluster
spark = SparkSession.builder \
    .appName("Test 2") \
    .master("spark://masterhost:7077") \
    .config("spark.executor.memory", "512mb") \
    .config("spark.executor.cores", "1").config("spark.num.executors", "1")\
    .getOrCreate()

# Tạo một DataFrame đơn giản
data = [("Alice", 34), ("Bob", 45), ("Cathy", 28)]
columns = ["Name", "Age"]

df = spark.createDataFrame(data, columns)

# Hiển thị DataFrame
df.show()

# Thực hiện một số phép toán trên DataFrame
df_filtered = df.filter(df.Age > 30)

# Hiển thị kết quả sau khi lọc
df_filtered.show()

# Dừng Spark session
spark.stop()
