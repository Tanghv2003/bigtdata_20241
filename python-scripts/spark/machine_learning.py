from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from graphframes import GraphFrame
from pyspark.sql.types import *

from pyspark.ml.feature import StringIndexer, OneHotEncoder, VectorAssembler, StandardScaler, MinMaxScaler
from pyspark.ml import Pipeline
from pyspark.ml.evaluation import BinaryClassificationEvaluator

import matplotlib.pyplot as plt
import seaborn as sns
from sklearn.metrics import roc_curve, auc
from pyspark.ml.stat import Correlation
from pyspark.sql.functions import when, col

# Tạo SparkSession
spark = SparkSession.builder \
    .appName("Airlines Machine Learning") \
    .config("spark.es.nodes", "elasticsearch") \
    .config("spark.es.port", "9200") \
    .config("spark.es.index.auto.create", "true") \
    .config("spark.jars.packages", "graphframes:graphframes:0.8.2-spark3.0-s_2.12") \
    .master("spark://spark-master:7077") \
    .getOrCreate()

# HDFS file path
hdfs_file_path = "hdfs://namenode:8020/upload/data.csv"

# Đọc dữ liệu vào DataFrame
df = spark.read.option("header", "true").csv(hdfs_file_path)

# tiền xử lí dữ liệu : xóa null -> lọc trùng -> cân bằng dữ liệu nếu cần

num_data_missing_arr_delay = df.filter("ArrDelay IS NULL").count()
print(f"Số dòng thiếu giá trị ArrDelay: {num_data_missing_arr_delay}")


flightdata_delayed = df.filter((df.ArrDelay.isNotNull()) & (df.ArrDelay != "NA"))
print(f"Số dòng dữ liệu sau khi xóa là {flightdata_delayed.count()}")

flightdata_delayed_label = flightdata_delayed.withColumn(
    'Label', 
    (col('ArrDelay') >= 15).cast(IntegerType())
)

flightdata_delayed_label.select("ArrDelay", "Label").show(10)



########################
delay_each_airline = flightdata_delayed_label.filter(flightdata_delayed_label["Label"] == 1) \
    .groupBy("UniqueCarrier","Label").count().withColumnRenamed("count", "Delay")
delay_each_airline.show(10)

total_each_airline = flightdata_delayed_label.groupBy("UniqueCarrier").count().withColumnRenamed("count", "Total")
total_each_airline.show(10)

delay_percentage = delay_each_airline.join(total_each_airline, on="UniqueCarrier") \
    .withColumn("DelayPercentage", (delay_each_airline["Delay"] / total_each_airline["Total"]) * 100) ## update gửi kq về database
delay_percentage.show(10)

# xử lí mất cân bằng dữ liệu

print('Trước khi cân bằng:')
flightdata_delayed_label.groupBy('Label').count().show()

major_class_flights = flightdata_delayed_label.filter(flightdata_delayed_label.Label == 0)
minor_class_flights = flightdata_delayed_label.filter(flightdata_delayed_label.Label == 1)

ratio = minor_class_flights.count() / major_class_flights.count()
balanced_flights_delayed = major_class_flights.sample(withReplacement=True, fraction=ratio, seed=631).union(minor_class_flights)

print('Sau khi cân bằng:')
balanced_flights_delayed.groupBy('label').count().show()

# hồi quy logistic
# one hot encoding
# flightdata_delayed_label = flightdata_delayed_label.withColumn("CancellationCode", 
#                   when(col("CancellationCode").isNull() | (col("CancellationCode") == "NA"), "UNKNOWN")
#                   .otherwise(col("CancellationCode")))

# # Kiểm tra lại các giá trị duy nhất trong cột CancellationCode
# flightdata_delayed_label.select("CancellationCode").distinct().show()

#one hot encoding cho các cột UniqueCarrier, Origin, Dest

airline_indexer = StringIndexer(inputCol="UniqueCarrier", outputCol="UniqueCarrierIndex")
airline_encoder = OneHotEncoder(inputCol="UniqueCarrierIndex", outputCol="UniqueCarrierVec")

origin_indexer = StringIndexer(inputCol="Origin", outputCol="OriginIndex")
origin_encoder = OneHotEncoder(inputCol="OriginIndex", outputCol="OriginVec")

destination_indexer = StringIndexer(inputCol="Dest", outputCol="DestIndex")
destination_encoder = OneHotEncoder(inputCol="DestIndex", outputCol="DestVec")


assembler_one_hot = VectorAssembler(inputCols=["UniqueCarrierVec", "OriginVec", "DestVec"], outputCol="OneHotVec")

# Tạo vector cho các cột số thực
assembler_numeric = VectorAssembler(inputCols=["Month", "DayofMonth", "FlightNum", "CRSDepTime", "DepDelay", 
                                               "Distance", "CRSArrTime", "Diverted", "Cancelled"], 
                                    outputCol="NumericVec")

# Kết hợp các cột one-hot và các cột số thực
final_assembler = VectorAssembler(inputCols=["OneHotVec", "NumericVec"], outputCol="RAW_FEATURES")

# Khai báo StandardScaler
scaler = StandardScaler(inputCol="RAW_FEATURES", outputCol="FEATURES", withStd=True, withMean=False)

# Đưa vào pipeline
pipeline_encoder = Pipeline(stages=[
    airline_indexer, airline_encoder,
    origin_indexer, origin_encoder,
    destination_indexer, destination_encoder,
    assembler_one_hot, assembler_numeric, final_assembler, scaler
])

# Fit và transform dữ liệu
pipeline_model_encoder = pipeline_encoder.fit(balanced_flights_delayed)





