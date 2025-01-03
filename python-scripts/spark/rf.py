from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, from_unixtime, unix_timestamp, lpad
from pyspark.ml.feature import StringIndexer, OneHotEncoder, VectorAssembler, Imputer
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml import Pipeline
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from pyspark.ml.tuning import CrossValidator, ParamGridBuilder

# Create Spark session
spark = SparkSession.builder \
    .appName("Airlines Data Processing") \
    .config("spark.es.nodes", "elasticsearch") \
    .config("spark.es.port", "9200") \
    .config("spark.es.index.auto.create", "true") \
    .config("spark.jars.packages", "graphframes:graphframes:0.8.2-spark3.0-s_2.12") \
    .master("spark://spark-master:7077") \
    .getOrCreate()

# Load data
hdfs_file_path = "hdfs://namenode:8020/upload/data.csv"
df = spark.read.option("header", "true").csv(hdfs_file_path)

# Define schema
schema = [
    ('ActualElapsedTime', 'double'),
    ('AirTime', 'double'),
    ('ArrDelay', 'double'),
    ('ArrTime', 'string'),
    ('CRSArrTime', 'string'),
    ('CRSDepTime', 'string'),
    ('CRSElapsedTime', 'double'),
    ('CancellationCode', 'string'),
    ('Cancelled', 'int'),
    ('CarrierDelay', 'double'),
    ('DayOfWeek', 'int'),
    ('DayofMonth', 'int'),
    ('DepDelay', 'double'),
    ('DepTime', 'string'),
    ('Dest', 'string'),
    ('Distance', 'double'),
    ('Diverted', 'string'),
    ('FlightNum', 'int'),
    ('LateAircraftDelay', 'double'),
    ('Month', 'int'),
    ('NASDelay', 'double'),
    ('Origin', 'string'),
    ('SecurityDelay', 'double'),
    ('TailNum', 'string'),
    ('TaxiIn', 'double'),
    ('TaxiOut', 'double'),
    ('UniqueCarrier', 'string'),
    ('WeatherDelay', 'double'),
    ('Year', 'int')
]

# Convert columns based on schema
for column_name, column_type in schema:
    if column_name in df.columns:
        df = df.withColumn(column_name,
                           when(col(column_name).isin(["NA", ""]), None)
                           .otherwise(col(column_name).cast(column_type)))
    else:
        print(f"Column {column_name} not found in DataFrame")

# Process missing values and data conversion
for column_name, column_type in schema:
    if column_type == 'string':
        if column_name in ['ArrTime', 'CRSArrTime', 'DepTime', 'CRSDepTime']:
            df = df.withColumn(column_name,
                               when(col(column_name).isin(["NA", ""]), None)
                               .otherwise(from_unixtime(unix_timestamp(lpad(col(column_name), 4, '0'), 'HHmm'), 'HH:mm')))
        else:
            df = df.withColumn(column_name,
                               when(col(column_name).isin(["NA", ""]), None)
                               .otherwise(col(column_name)))
    else:
        df = df.withColumn(column_name,
                           when(col(column_name).isin(["NA", ""]), None)
                           .otherwise(col(column_name).cast(column_type)))

# Drop rows with missing ArrDelay
df = df.na.drop(subset=["ArrDelay"])

# Create delay categories
df = df.withColumn("DelayCategory",
    when(col("ArrDelay") < 15, 0)  # 0-15 minutes
    .when((col("ArrDelay") >= 15) & (col("ArrDelay") < 45), 1)  # 15-45 minutes
    .when((col("ArrDelay") >= 45) & (col("ArrDelay") < 90), 2)  # 45-90 minutes
    .when((col("ArrDelay") >= 90) & (col("ArrDelay") < 150), 3)  # 90-150 minutes
    .when((col("ArrDelay") >= 150) & (col("ArrDelay") < 240), 4)  # 150-240 minutes
    .otherwise(5)  # >240 minutes (4 hours)
)

# Define categorical and numeric features
categorical_features = ["UniqueCarrier", "Origin", "Dest", "Diverted"]
numeric_features = [
    "Month", "DayofMonth", "DayOfWeek", "Year",
    "CRSElapsedTime", "Distance",
    "DepDelay"
]

# Create StringIndexer and OneHotEncoder stages
indexers = [StringIndexer(inputCol=feat, outputCol=f"{feat}Index", handleInvalid="keep")
            for feat in categorical_features]
encoders = [OneHotEncoder(inputCol=f"{feat}Index", outputCol=f"{feat}Vec", handleInvalid="keep")
            for feat in categorical_features]

# Create VectorAssembler stage
assembler = VectorAssembler(
    inputCols=numeric_features + [f"{feat}Vec" for feat in categorical_features],
    outputCol="features",
    handleInvalid="keep"
)

# Create Imputer stage
imputer = Imputer(
    inputCols=numeric_features,
    outputCols=numeric_features,  # Replace directly in the columns
    strategy="mean"  # Or "median" depending on the data
)

# Create RandomForestClassifier stage
rf = RandomForestClassifier(featuresCol="features", labelCol="DelayCategory", seed=42)

# Create pipeline
pipeline = Pipeline(stages=indexers + encoders + [imputer, assembler, rf])

# Split data into training and testing
train_data, test_data = df.randomSplit([0.8, 0.2], seed=42)

# Define parameter grid for cross-validation
paramGrid = (ParamGridBuilder()
             .addGrid(rf.numTrees, [50, 100, 150])  # Number of trees
             .addGrid(rf.maxDepth, [5, 10, 15])     # Depth of trees
             .build())

# Create evaluators
evaluator_accuracy = MulticlassClassificationEvaluator(
    labelCol="DelayCategory",
    predictionCol="prediction",
    metricName="accuracy"
)
evaluator_weighted_precision = MulticlassClassificationEvaluator(
    labelCol="DelayCategory",
    predictionCol="prediction",
    metricName="weightedPrecision"
)

# Create CrossValidator
crossval = CrossValidator(
    estimator=pipeline,
    estimatorParamMaps=paramGrid,
    evaluator=evaluator_accuracy,  # Use accuracy to select the best model
    numFolds=5,  # Number of k-folds
    parallelism=4  # Parallelism
)

# Fit the cross-validator
cvModel = crossval.fit(train_data)

# Get the best model
bestModel = cvModel.bestModel

# Save the best model to HDFS
model_path = "hdfs://namenode:8020/models/best_rf_model"
bestModel.save(model_path)

# Load the model for future use
from pyspark.ml.pipeline import PipelineModel
loaded_model = PipelineModel.load(model_path)

# Generate predictions using the loaded model
predictions = loaded_model.transform(test_data)

# Evaluate the loaded model
accuracy = evaluator_accuracy.evaluate(predictions)
weighted_precision = evaluator_weighted_precision.evaluate(predictions)

print(f"Loaded Model Accuracy: {accuracy}")
print(f"Loaded Model Weighted Precision: {weighted_precision}")

# Print confusion matrix
print("\nConfusion Matrix:")
predictions.groupBy("DelayCategory", "prediction") \
    .count() \
    .orderBy("DelayCategory", "prediction") \
    .show()