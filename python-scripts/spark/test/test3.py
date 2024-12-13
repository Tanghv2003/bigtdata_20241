from pyspark.sql import SparkSession

# Tạo một SparkSession và chỉ định Spark Master
spark = SparkSession.builder \
    .appName("Submit to Spark Master") \
    .master("spark://spark-master:7077") \
    .getOrCreate()

# Kiểm tra nếu SparkSession đã được tạo thành công
if spark.version:
    print(f"Spark is running! Version: {spark.version}")
else:
    print("Spark session could not be created.")

# Thực hiện một phép toán đơn giản trên RDD
rdd = spark.sparkContext.parallelize([1, 2, 3, 4, 5])
sum_result = rdd.reduce(lambda a, b: a + b)

print(f"Sum of elements in the RDD: {sum_result}")

# Dừng SparkSession sau khi hoàn thành
spark.stop()
