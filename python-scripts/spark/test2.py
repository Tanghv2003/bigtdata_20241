from pyspark.sql import SparkSession

# Tạo SparkSession
spark = SparkSession.builder \
    .appName("Spark to Elasticsearch Example") \
    .config("spark.es.nodes", "elasticsearch") \
    .config("spark.es.port", "9200") \
    .config("spark.es.index.auto.create", "true") \
    .config("spark.jars.packages", "org.elasticsearch:elasticsearch-spark-30_2.12:8.0.0") \
    .getOrCreate()

# Tạo một DataFrame từ dữ liệu
data = [("John", 30), ("Jane", 25), ("Jake", 35)]
columns = ["name", "age"]
df = spark.createDataFrame(data, columns)

# Đẩy dữ liệu vào Elasticsearch
df.write \
    .format("org.elasticsearch.spark.sql") \
    .option("es.resource", "people/records") \
    .mode("overwrite") \
    .save()

spark.stop()
