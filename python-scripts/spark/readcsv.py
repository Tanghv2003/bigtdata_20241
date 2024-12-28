from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when
from pyspark.sql.types import IntegerType, StringType, DoubleType
from pyspark.ml.feature import StringIndexer, OneHotEncoder, VectorAssembler, StandardScaler
from pyspark.ml import Pipeline
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.evaluation import BinaryClassificationEvaluator


spark = SparkSession.builder \
    .appName("Airlines Data Processing") \
    .config("spark.es.nodes", "elasticsearch") \
    .config("spark.es.port", "9200") \
    .config("spark.es.index.auto.create", "true") \
    .config("spark.jars.packages", "graphframes:graphframes:0.8.2-spark3.0-s_2.12") \
    .master("spark://spark-master:7077") \
    .getOrCreate()


hdfs_file_path = "hdfs://namenode:8020/upload/data.csv"
df = spark.read.option("header", "true").csv(hdfs_file_path)


schema = [
    ('ActualElapsedTime', 'double'),
    ('AirTime', 'double'),
    ('ArrDelay', 'double'),
    ('ArrTime', 'double'),
    ('CRSArrTime', 'double'),
    ('CRSDepTime', 'double'),
    ('CRSElapsedTime', 'double'),
    ('CancellationCode', 'string'),
    ('Cancelled', 'double'),
    ('CarrierDelay', 'double'),
    ('DayOfWeek', 'double'),
    ('DayofMonth', 'double'),
    ('DepDelay', 'double'),
    ('DepTime', 'double'),
    ('Dest', 'string'),
    ('Distance', 'double'),
    ('Diverted', 'double'),
    ('FlightNum', 'double'),
    ('LateAircraftDelay', 'double'),
    ('Month', 'double'),
    ('NASDelay', 'double'),
    ('Origin', 'string'),
    ('SecurityDelay', 'double'),
    ('TailNum', 'string'),
    ('TaxiIn', 'double'),
    ('TaxiOut', 'double'),
    ('UniqueCarrier', 'string'),
    ('WeatherDelay', 'double'),
    ('Year', 'double')
]


for column_name, column_type in schema:
    if column_type == 'string':
        df = df.withColumn(column_name, 
                          when(col(column_name).isin(["NA", ""]), None)
                          .otherwise(col(column_name)))
    else:
        df = df.withColumn(column_name, 
                          when(col(column_name).isin(["NA", ""]), None)
                          .otherwise(col(column_name).cast(column_type)))


df = df.na.drop(subset=["ArrDelay"])
df = df.withColumn("Label", (col("ArrDelay") >= 15).cast(IntegerType()))


delayed_flights = df.filter(df.Label == 1)
non_delayed_flights = df.filter(df.Label == 0)
ratio = delayed_flights.count() / non_delayed_flights.count()
balanced_df = non_delayed_flights.sample(withReplacement=True, fraction=ratio, seed=42) \
    .union(delayed_flights)


categorical_stages = []
categorical_output_cols = []

for feature in ["UniqueCarrier", "Origin", "Dest"]:
    indexer = StringIndexer(inputCol=feature, 
                          outputCol=f"{feature}Index",
                          handleInvalid="keep")
    encoder = OneHotEncoder(inputCol=f"{feature}Index",
                          outputCol=f"{feature}Vec",
                          handleInvalid="keep")
    categorical_stages.extend([indexer, encoder])
    categorical_output_cols.append(f"{feature}Vec")


numeric_features = ["Month", "DayofMonth", "FlightNum", "CRSDepTime",
                   "DepDelay", "Distance", "CRSArrTime", "Diverted", "Cancelled"]


assembler = VectorAssembler(
    inputCols=numeric_features + categorical_output_cols,
    outputCol="RAW_FEATURES",
    handleInvalid="keep"
)


scaler = StandardScaler(
    inputCol="RAW_FEATURES",
    outputCol="FEATURES",
    withStd=True,
    withMean=False
)


lr = LogisticRegression(
    featuresCol="FEATURES",
    labelCol="Label",
    maxIter=10,
    regParam=0.1
)


pipeline = Pipeline(stages=categorical_stages + [assembler, scaler, lr])


train_df, test_df = balanced_df.randomSplit([0.8, 0.2], seed=42)

try:
    
    
    model = pipeline.fit(train_df)
    
    
    hdfs_model_path = "hdfs://namenode:8020/models/logistic_model"
    model.write().overwrite().save(hdfs_model_path)
    print(f"Model saved successfully at: {hdfs_model_path}")
    
    
    predictions = model.transform(test_df)
    evaluator = BinaryClassificationEvaluator(
        labelCol="Label",
        rawPredictionCol="rawPrediction",
        metricName="areaUnderROC"
    )
    
    roc_auc = evaluator.evaluate(predictions)
    print(f"ROC AUC: {roc_auc}")
    
except Exception as e:
    print(f"Error during training: {str(e)}")
finally:
    spark.stop()
