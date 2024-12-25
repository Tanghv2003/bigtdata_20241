from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from graphframes import GraphFrame

# Tạo SparkSession
spark = SparkSession.builder \
    .appName("Airlines GraphFrames") \
    .config("spark.es.nodes", "elasticsearch") \
    .config("spark.es.port", "9200") \
    .config("spark.es.index.auto.create", "true") \
    .config("spark.jars.packages", "graphframes:graphframes:0.8.2-spark3.0-s_2.12") \
    .appName("Python Spark Job test") \
    .master("spark://spark-master:7077") \
    .getOrCreate()

# Thiết lập thư mục checkpoint
checkpoint_dir = "hdfs://namenode:8020/tmp/checkpoint"
spark.sparkContext.setCheckpointDir(checkpoint_dir)

# HDFS file path
hdfs_file_path = "hdfs://namenode:8020/upload/data.csv"

# Đọc dữ liệu vào DataFrame
df = spark.read.option("header", "true").csv(hdfs_file_path)

# Chuyển đổi các cột sang kiểu dữ liệu phù hợp
df = df.withColumn("Distance", col("Distance").cast("int")) \
    .withColumn("DepDelay", col("DepDelay").cast("int")) \
    .withColumn("ArrDelay", col("ArrDelay").cast("int"))

# Tạo các đỉnh (sân bay) từ các cột 'Origin' và 'Dest'
vertices = df.select("Origin").distinct().withColumnRenamed("Origin", "id") \
    .union(df.select("Dest").distinct().withColumnRenamed("Dest", "id"))

# Hiển thị các đỉnh
vertices.show()

# Tạo các cạnh giữa các sân bay (Origin -> Dest)
edges = df.select(
    col("Origin").alias("src"), 
    col("Dest").alias("dst"),
    col("Distance").alias("distance"),  # Thuộc tính trọng số là 'Distance'
    col("DepDelay").alias("departure_delay"),  # Thuộc tính delay khởi hành
    col("ArrDelay").alias("arrival_delay")  # Thuộc tính delay đến
)

# Hiển thị các cạnh
edges.show()

# Tạo GraphFrame từ các đỉnh và các cạnh
graph = GraphFrame(vertices, edges)

# Hiển thị thông tin về đồ thị
print(f"Đỉnh: {graph.vertices.count()}")
print(f"Cạnh: {graph.edges.count()}")

# Áp dụng thuật toán PageRank chỉ ra sân bay bận rộn nhất
page_rank = graph.pageRank(resetProbability=0.15, maxIter=10)

# Hiển thị giá trị PageRank của các đỉnh
print("PageRank của các sân bay:")
page_rank.vertices.select("id", "pagerank").show()


# Dừng Spark session
spark.stop()
