from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, count, month, avg, sum, countDistinct, when, round, 
    desc, asc, year, dayofweek, hour, expr, stddev, min, max,
    percentile_approx, collect_set, concat, lit, length, to_timestamp,
    substring, coalesce
)
from pyspark.sql.window import Window

# Initialize Spark session with local mode for testing
from pyspark.sql import SparkSession

# Khởi tạo Spark session với cấu hình cho 3 worker, mỗi worker có 2 executors, mỗi executor có 1 core
spark = SparkSession.builder \
    .appName("3 worker * 2 executor * 1 core ") \
    .config("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.12:3.0.2") \
    .config("spark.master", "spark://spark-master:7077") \
    .config("spark.executor.instances", "6") \
    .config("spark.executor.cores", "1") \
    .config("spark.cores.max", "6") \
    .config("spark.executor.memory", "512mb") \
    .getOrCreate()

# Tiến hành các phép biến đổi và hành động trên DataFrame của bạn...


# # Set log level to show more details
# spark.sparkContext.setLogLevel("INFO")

# HDFS file path
hdfs_file_path = "hdfs://namenode:8020/upload/data.csv"

# Enhanced error handling for reading CSV+
try:
    # Read the data into a DataFrame
    df = spark.read.option("header", "true").csv(hdfs_file_path)
except Exception as e:
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
        raise e

df = transform_dataframe(df)



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

    print("1\n")
    # Display the result
    carrier_stats.show()

    
except Exception as e:
    raise e

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
    print("2\n")
    # Display the result
    route_stats.show()

   
except Exception as e:
    raise e

# 3. Delay Type Analysis
try:
    delay_analysis = df.groupBy("OP_CARRIER").agg(
        round(avg("CARRIER_DELAY"), 2).alias("avg_carrier_delay"),
        round(avg("WEATHER_DELAY"), 2).alias("avg_weather_delay"),
        round(avg("NAS_DELAY"), 2).alias("avg_nas_delay"),
        round(avg("SECURITY_DELAY"), 2).alias("avg_security_delay"),
        round(avg("LATE_AIRCRAFT_DELAY"), 2).alias("avg_late_aircraft_delay")
    ).cache()
    print("3\n")
    # Display the result
    delay_analysis.show()

#     write_to_mongodb(delay_analysis, "delay_type_analysis")
except Exception as e:
    raise e

# 4. Airport Performance
try:
    # Optimize memory by selecting only needed columns
    df_airport = df.select("ORIGIN", "DEP_DELAY", "TAXI_OUT", "TAXI_IN", "CANCELLED", "OP_CARRIER", "DEST") \
                  .na.fill(0, ["DEP_DELAY", "TAXI_OUT", "TAXI_IN", "CANCELLED"])
    
    # Split the aggregations into smaller chunks
    base_metrics = df_airport.groupBy("ORIGIN").agg(
        count("*").alias("total_departures"),
        round(avg(col("DEP_DELAY").cast("double")), 2).alias("avg_departure_delay"),
        round(avg(col("TAXI_OUT").cast("double")), 2).alias("avg_taxi_out_time"),
        round(avg(col("TAXI_IN").cast("double")), 2).alias("avg_taxi_in_time")
    )
    
    carrier_metrics = df_airport.groupBy("ORIGIN").agg(
        countDistinct("OP_CARRIER").alias("num_carriers"),
        countDistinct("DEST").alias("num_destinations")
    )
    
    delay_metrics = df_airport.groupBy("ORIGIN").agg(
        round(avg(col("CANCELLED").cast("double")), 4).alias("cancellation_rate"),
        round(max(col("DEP_DELAY").cast("double")), 2).alias("max_departure_delay"),
        round(stddev(col("DEP_DELAY").cast("double")), 2).alias("departure_delay_std")
    )
    
    # Join all metrics together
    airport_stats = base_metrics.join(carrier_metrics, "ORIGIN") \
                               .join(delay_metrics, "ORIGIN") \
                               .orderBy(col("total_departures").desc())
    print("4\n")
    airport_stats.limit(5).show()
    
    #write_to_mongodb(airport_stats, "airport_performance")
except Exception as e:
    raise e

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
    print("5\n")
    time_analysis.show()

    # write_to_mongodb(time_analysis, "time_based_analysis")
except Exception as e:
    raise e

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
    print("1\n")
    distance_analysis.show()

    # write_to_mongodb(distance_analysis, "distance_based_analysis")
except Exception as e:
    raise e

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
    print("7\n")
    delay_severity.show()

    # write_to_mongodb(delay_severity, "delay_severity_analysis")
except Exception as e:
    raise e

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
    print("8\n")
    performance_metrics.show()

    # write_to_mongodb(performance_metrics, "overall_performance_metrics")
except Exception as e:
    raise e

# Stop Spark session
try:
    spark.stop()
except Exception as e:
    raise e