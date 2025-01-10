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
    .master("spark://spark-master:7077") \
    .getOrCreate()

# Thiết lập thư mục checkpoint
checkpoint_dir = "hdfs://namenode:8020/tmp/checkpoint"
spark.sparkContext.setCheckpointDir(checkpoint_dir)

# HDFS file path
hdfs_file_path = "hdfs://namenode:8020/upload/2009.csv"

# Đọc dữ liệu vào DataFrame với schema phù hợp
df = spark.read \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .csv(hdfs_file_path)

# Chuyển đổi các cột sang kiểu dữ liệu phù hợp và chọn các cột cần thiết
df_cleaned = df.select(
    "FL_DATE",
    "OP_CARRIER",
    "OP_CARRIER_FL_NUM",
    "ORIGIN",
    "DEST",
    col("DEP_DELAY").cast("double"),
    col("ARR_DELAY").cast("double"),
    col("DISTANCE").cast("double"),
    col("CANCELLED").cast("double"),
    col("DIVERTED").cast("double")
).na.fill(0)  # Điền giá trị null bằng 0

# Tạo các đỉnh (sân bay) từ các cột 'ORIGIN' và 'DEST'
vertices = df_cleaned.select("ORIGIN").distinct().withColumnRenamed("ORIGIN", "id") \
    .union(df_cleaned.select("DEST").distinct().withColumnRenamed("DEST", "id"))

# Tạo các cạnh với thông tin chi tiết hơn
edges = df_cleaned.select(
    col("ORIGIN").alias("src"),
    col("DEST").alias("dst"),
    col("DISTANCE").alias("distance"),
    col("DEP_DELAY").alias("departure_delay"),
    col("ARR_DELAY").alias("arrival_delay"),
    col("OP_CARRIER").alias("carrier"),
    col("CANCELLED").alias("cancelled"),
    col("DIVERTED").alias("diverted")
)

# Tạo GraphFrame từ các đỉnh và các cạnh
graph = GraphFrame(vertices, edges)

# Hiển thị thông tin cơ bản về đồ thị
print("Thống kê đồ thị:")
print(f"Số lượng sân bay (đỉnh): {graph.vertices.count()}")
print(f"Số lượng chuyến bay (cạnh): {graph.edges.count()}")

# Áp dụng PageRank để xác định sân bay quan trọng nhất
page_rank = graph.pageRank(resetProbability=0.15, maxIter=10)

# Hiển thị top 10 sân bay có PageRank cao nhất
print("\nTop 10 sân bay quan trọng nhất:")
page_rank.vertices \
    .select("id", "pagerank") \
    .orderBy(col("pagerank").desc()) \
    .limit(10) \
    .show()



# Dừng Spark session
spark.stop()