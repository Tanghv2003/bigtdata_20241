from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, count, month, avg, sum, countDistinct, when, round, 
    desc, asc, year, dayofweek, hour, expr, stddev, min, max,
    percentile_approx, collect_set, concat, lit, length, to_timestamp,
    substring, coalesce
)
from pyspark.sql.window import Window

# Initialize Spark session with local mode for testing
spark = SparkSession.builder \
    .appName("Flight Data Analysis") \
    .config("spark.mongodb.write.connection.uri", "mongodb+srv://bigdata:bigdata2024@bigdata.axiis.mongodb.net/flight_stats?retryWrites=true&w=majority&appName=bigdata") \
    .config("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.12:3.0.2") \
    .config("spark.mongodb.output.maxBatchSize", "4096") \
    .config("spark.sql.legacy.timeParserPolicy", "LEGACY") \
    .config("spark.sql.shuffle.partitions", "10") \
    .config("spark.default.parallelism", "10") \
    .config("spark.executor.memory", "1g") \
    .config("spark.driver.memory", "1g") \
    .config("spark.memory.offHeap.enabled", "true") \
    .config("spark.memory.offHeap.size", "1g") \
    .config("spark.driver.maxResultSize", "1g") \
    .config("spark.memory.fraction", "0.7") \
    .config("spark.memory.storageFraction", "0.3") \
    .config("spark.dynamicAllocation.enabled", "true") \
    .config("spark.dynamicAllocation.initialExecutors", "1") \
    .config("spark.dynamicAllocation.minExecutors", "1") \
    .config("spark.dynamicAllocation.maxExecutors", "2") \
    .master("local[*]") \
    .getOrCreate()

# Set log level to show more details
spark.sparkContext.setLogLevel("INFO")

print("Spark session created successfully")

# HDFS file path
hdfs_file_path = "hdfs://namenode:8020/upload/flight_data.csv"

# Enhanced error handling for reading CSV
try:
    # Read the data into a DataFrame
    df = spark.read.option("header", "true").csv(hdfs_file_path)
except Exception as e:
    print(f"Error reading CSV file: {str(e)}")
    raise e

# Improved data transformation with error handling for time fields
def transform_dataframe(df):
    try:
        return df \
            .withColumn("FL_DATE", to_timestamp(col("FL_DATE"))) \
            .withColumn("DEP_DELAY", col("DEP_DELAY").cast("double")) \
            .withColumn("ARR_DELAY", col("ARR_DELAY").cast("double")) \
            .withColumn("DISTANCE", col("DISTANCE").cast("double")) \
            .withColumn("AIR_TIME", col("AIR_TIME").cast("double")) \
            .withColumn("TAXI_IN", col("TAXI_IN").cast("double")) \
            .withColumn("TAXI_OUT", col("TAXI_OUT").cast("double")) \
            .withColumn("CARRIER_DELAY", coalesce(col("CARRIER_DELAY").cast("double"), lit(0.0))) \
            .withColumn("WEATHER_DELAY", coalesce(col("WEATHER_DELAY").cast("double"), lit(0.0))) \
            .withColumn("NAS_DELAY", coalesce(col("NAS_DELAY").cast("double"), lit(0.0))) \
            .withColumn("SECURITY_DELAY", coalesce(col("SECURITY_DELAY").cast("double"), lit(0.0))) \
            .withColumn("LATE_AIRCRAFT_DELAY", coalesce(col("LATE_AIRCRAFT_DELAY").cast("double"), lit(0.0))) \
            .withColumn("CANCELLED", col("CANCELLED").cast("double")) \
            .withColumn("DIVERTED", col("DIVERTED").cast("double")) \
            .withColumn("CRS_ELAPSED_TIME", col("CRS_ELAPSED_TIME").cast("double")) \
            .withColumn("ACTUAL_ELAPSED_TIME", col("ACTUAL_ELAPSED_TIME").cast("double")) \
            .withColumn("DEP_HOUR",
                when(length(col("DEP_TIME")) == 4, 
                    expr("cast(substring(DEP_TIME, 1, 2) as int)"))
                .when(length(col("DEP_TIME")) == 3,
                    expr("cast(substring(concat('0', DEP_TIME), 1, 2) as int)"))
                .otherwise(lit(0))
            )
    except Exception as e:
        print(f"Error in data transformation: {str(e)}")
        raise e

df = transform_dataframe(df)

# Function to write DataFrame to MongoDB with retries
def write_to_mongodb(df, collection_name, max_retries=3):
    for attempt in range(max_retries):
        try:
            print(f"Attempt {attempt + 1} to write to {collection_name}")
            
            # Cache DataFrame
            df_cached = df.cache()
            row_count = df_cached.count()
            
            if row_count == 0:
                print(f"Warning: No data to write for collection {collection_name}")
                return
                
            write_uri = "mongodb+srv://bigdata:bigdata2024@bigdata.axiis.mongodb.net/flight_stats"
            
            # Write with smaller batch size
            df_cached.write \
                .format("mongo") \
                .mode("overwrite") \
                .option("uri", write_uri) \
                .option("database", "flight_stats") \
                .option("collection", collection_name) \
                .option("maxBatchSize", 1024) \
                .save()
                
            print(f"Successfully wrote {row_count} rows to collection {collection_name}")
            df_cached.unpersist()
            return
            
        except Exception as e:
            print(f"Error on attempt {attempt + 1} writing to {collection_name}: {str(e)}")
            if attempt == max_retries - 1:
                raise e
            else:
                print("Retrying...")

# 1. Carrier Performance Analysis
try:
    # Handle missing values first
    df_clean = df.na.fill(0, ["ARR_DELAY", "DEP_DELAY", "CARRIER_DELAY", 
                             "WEATHER_DELAY", "CANCELLED", "DIVERTED",
                             "ACTUAL_ELAPSED_TIME", "CRS_ELAPSED_TIME"])
    
    carrier_stats = df_clean.groupBy("OP_CARRIER").agg(
        count("*").alias("total_flights"),
        round(avg(col("ARR_DELAY").cast("double")), 2).alias("avg_arrival_delay"),
        round(avg(col("DEP_DELAY").cast("double")), 2).alias("avg_departure_delay"),
        round(avg(col("AIR_TIME").cast("double")), 2).alias("avg_air_time"),
        round(avg(col("DISTANCE").cast("double")), 2).alias("avg_distance"),
        round(avg(col("CARRIER_DELAY").cast("double")), 2).alias("avg_carrier_delay"),
        round(avg(col("WEATHER_DELAY").cast("double")), 2).alias("avg_weather_delay"),
        round(avg(col("CANCELLED").cast("double")), 4).alias("cancellation_rate"),
        round(avg(col("DIVERTED").cast("double")), 4).alias("diversion_rate"),
        countDistinct("ORIGIN").alias("unique_origins"),
        countDistinct("DEST").alias("unique_destinations"),
        round((sum(when(col("ARR_DELAY").cast("double") <= 0, 1).otherwise(0)) / count("*") * 100), 2).alias("on_time_performance"),
        round(avg(when(col("ARR_DELAY").cast("double") > 15, 1).otherwise(0)) * 100, 2).alias("significant_delay_percentage"),
        round(avg(col("ACTUAL_ELAPSED_TIME").cast("double") - col("CRS_ELAPSED_TIME").cast("double")), 2).alias("avg_time_deviation")
    ).cache()

    write_to_mongodb(carrier_stats, "carrier_performance")
except Exception as e:
    print(f"Error in carrier performance analysis: {str(e)}")

# 2. Route Analysis
try:
    route_stats = df.withColumn("ROUTE", concat(col("ORIGIN"), lit("-"), col("DEST"))) \
        .groupBy("ROUTE", "ORIGIN", "DEST").agg(
            count("*").alias("flight_count"),
            round(avg("DISTANCE"), 2).alias("distance"),
            round(avg("AIR_TIME"), 2).alias("avg_air_time"),
            round(avg("CRS_ELAPSED_TIME"), 2).alias("scheduled_time"),
            round(avg("ACTUAL_ELAPSED_TIME"), 2).alias("actual_time"),
            round(avg("ARR_DELAY"), 2).alias("avg_arrival_delay"),
            round(stddev("ARR_DELAY"), 2).alias("delay_variability"),
            round(avg(col("CANCELLED")), 4).alias("cancellation_rate")
        ).cache()

    write_to_mongodb(route_stats, "route_analysis")
except Exception as e:
    print(f"Error in route analysis: {str(e)}")

# 3. Delay Type Analysis
try:
    delay_analysis = df.groupBy("OP_CARRIER").agg(
        round(avg("CARRIER_DELAY"), 2).alias("avg_carrier_delay"),
        round(avg("WEATHER_DELAY"), 2).alias("avg_weather_delay"),
        round(avg("NAS_DELAY"), 2).alias("avg_nas_delay"),
        round(avg("SECURITY_DELAY"), 2).alias("avg_security_delay"),
        round(avg("LATE_AIRCRAFT_DELAY"), 2).alias("avg_late_aircraft_delay")
    ).cache()

    write_to_mongodb(delay_analysis, "delay_type_analysis")
except Exception as e:
    print(f"Error in delay type analysis: {str(e)}")

# 4. Airport Performance
try:
    print("Starting airport performance analysis...")
    
    # Optimize memory by selecting only needed columns
    df_airport = df.select("ORIGIN", "DEP_DELAY", "TAXI_OUT", "TAXI_IN", "CANCELLED", "OP_CARRIER", "DEST") \
                  .na.fill(0, ["DEP_DELAY", "TAXI_OUT", "TAXI_IN", "CANCELLED"])
    
    print("Calculating base metrics...")
    # Split the aggregations into smaller chunks
    base_metrics = df_airport.groupBy("ORIGIN").agg(
        count("*").alias("total_departures"),
        round(avg(col("DEP_DELAY").cast("double")), 2).alias("avg_departure_delay"),
        round(avg(col("TAXI_OUT").cast("double")), 2).alias("avg_taxi_out_time"),
        round(avg(col("TAXI_IN").cast("double")), 2).alias("avg_taxi_in_time")
    )
    
    print("Calculating carrier metrics...")
    carrier_metrics = df_airport.groupBy("ORIGIN").agg(
        countDistinct("OP_CARRIER").alias("num_carriers"),
        countDistinct("DEST").alias("num_destinations")
    )
    
    print("Calculating delay metrics...")
    delay_metrics = df_airport.groupBy("ORIGIN").agg(
        round(avg(col("CANCELLED").cast("double")), 4).alias("cancellation_rate"),
        round(max(col("DEP_DELAY").cast("double")), 2).alias("max_departure_delay"),
        round(stddev(col("DEP_DELAY").cast("double")), 2).alias("departure_delay_std")
    )
    
    print("Joining metrics...")
    # Join all metrics together
    airport_stats = base_metrics.join(carrier_metrics, "ORIGIN") \
                               .join(delay_metrics, "ORIGIN") \
                               .orderBy(col("total_departures").desc())
    
    print("\nTop 5 airports by departures:")
    airport_stats.limit(5).show()
    
    print(f"\nTotal airports: {airport_stats.count()}")
    
    print("Writing to MongoDB...")
    write_to_mongodb(airport_stats, "airport_performance")
    
    print("Airport performance analysis completed successfully")

except Exception as e:
    print(f"Error in airport performance analysis: {str(e)}")
    print("Full error details:")
    import traceback
    print(traceback.format_exc())

# 5. Time-based Analysis
try:
    time_analysis = df.groupBy(
        month("FL_DATE").alias("MONTH"),
        dayofweek("FL_DATE").alias("DAY_OF_WEEK"),
        col("DEP_HOUR")
    ).agg(
        count("*").alias("flight_count"),
        round(avg("ARR_DELAY"), 2).alias("avg_arrival_delay"),
        round(avg("DEP_DELAY"), 2).alias("avg_departure_delay"),
        round(avg(col("CANCELLED")), 4).alias("cancellation_rate")
    ).na.fill(0).cache()

    write_to_mongodb(time_analysis, "time_based_analysis")
except Exception as e:
    print(f"Error in time-based analysis: {str(e)}")

# 6. Distance-based Analysis
try:
    df = df.withColumn("DISTANCE_CATEGORY", 
        when(col("DISTANCE") < 500, "Short Haul")
        .when(col("DISTANCE") < 1500, "Medium Haul")
        .otherwise("Long Haul"))

    distance_analysis = df.groupBy("DISTANCE_CATEGORY").agg(
        count("*").alias("flight_count"),
        round(avg("ARR_DELAY"), 2).alias("avg_arrival_delay"),
        round(avg("AIR_TIME"), 2).alias("avg_air_time"),
        round(avg("CRS_ELAPSED_TIME"), 2).alias("avg_scheduled_time"),
        round(avg("ACTUAL_ELAPSED_TIME"), 2).alias("avg_actual_time"),
        round(avg(col("CARRIER_DELAY")), 2).alias("avg_carrier_delay"),
        round(avg(col("WEATHER_DELAY")), 2).alias("avg_weather_delay"),
        round(avg(col("CANCELLED")), 4).alias("cancellation_rate")
    ).cache()

    write_to_mongodb(distance_analysis, "distance_based_analysis")
except Exception as e:
    print(f"Error in distance-based analysis: {str(e)}")

# 7. Delay Severity Analysis
try:
    delay_severity = df.select(
        when(col("CANCELLED") == 1, "Cancelled")
        .when(col("DIVERTED") == 1, "Diverted")
        .when(col("ARR_DELAY") <= 0, "On Time")
        .when(col("ARR_DELAY") <= 15, "Slight Delay")
        .when(col("ARR_DELAY") <= 60, "Moderate Delay")
        .otherwise("Severe Delay").alias("DELAY_CATEGORY"),
        "OP_CARRIER"
    ).groupBy("OP_CARRIER", "DELAY_CATEGORY").agg(
        count("*").alias("flight_count")
    ).cache()

    write_to_mongodb(delay_severity, "delay_severity_analysis")
except Exception as e:
    print(f"Error in delay severity analysis: {str(e)}")

# 8. Performance Metrics
try:
    # Use the cleaned DataFrame
    performance_metrics = df_clean.agg(
        count("*").alias("total_flights"),
        round(avg(col("ARR_DELAY").cast("double")), 2).alias("overall_avg_arrival_delay"),
        round(stddev(col("ARR_DELAY").cast("double")), 2).alias("arrival_delay_std"),
        round(avg(col("DEP_DELAY").cast("double")), 2).alias("overall_avg_departure_delay"),
        round(percentile_approx(col("ARR_DELAY").cast("double"), 0.95), 2).alias("95th_percentile_delay"),
        round(avg(col("CANCELLED").cast("double")), 4).alias("overall_cancellation_rate"),
        round(avg(col("DIVERTED").cast("double")), 4).alias("overall_diversion_rate"),
        round((sum(when(col("ARR_DELAY").cast("double") <= 0, 1).otherwise(0)) / count("*") * 100), 2).alias("on_time_percentage")
    ).cache()

    write_to_mongodb(performance_metrics, "overall_performance_metrics")
except Exception as e:
    print(f"Error in performance metrics analysis: {str(e)}")

# Stop Spark session
try:
    spark.stop()
except Exception as e:
    print(f"Error stopping Spark session: {str(e)}")