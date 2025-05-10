from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    from_json,
    col,
    window,
    current_timestamp,
    desc,
    lag,
    row_number,
)
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    DoubleType,
    TimestampType,
)
from pyspark.sql.window import Window


#  -----------------    Part 1 ----------------

#  Create SparkSession.
spark = SparkSession.builder.appName("TrafficMonitoring").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

schema = StructType(
    [
        StructField("sensor_id", StringType(), True),
        StructField("timestamp", StringType(), True),
        StructField("vehicle_count", IntegerType(), True),
        StructField("average_speed", DoubleType(), True),
        StructField("congestion_level", StringType(), True),
    ]
)
print("Defined schema:")
print(schema)

kafka_df = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("subscribe", "traffic_data")
    .option("startingOffsets", "latest")
    .option("failOnDataLoss", "false")
    .load()
)
print("Kafka stream created.")

json_df = kafka_df.selectExpr("CAST(value AS STRING) as json_str")
parsed_df = json_df.select(from_json(col("json_str"), schema).alias("data"))


events_df = parsed_df.select("data.*")
print("Parsed events_df schema:")
events_df.printSchema()

# -----------  Part 3    Data Quality Checks ----------------
clean_df = events_df.filter(col("sensor_id").isNotNull() & col("timestamp").isNotNull())
clean_df = clean_df.filter((col("vehicle_count") >= 0) & (col("average_speed") > 0))
clean_df = clean_df.dropDuplicates(["sensor_id", "timestamp"])
print("Cleaned data (logical plan):")
clean_df.printSchema()

# Convert timestamp string to actual TimestampType.
clean_df = clean_df.withColumn("timestamp", col("timestamp").cast(TimestampType()))


# ------------------ Part 2 Analytics -----------------------

# Analysis Task 1: Compute Real-Time Traffic Volume per Sensor (5-minute window).
traffic_volume = (
    clean_df.groupBy("sensor_id", window(col("timestamp"), "5 minutes"))
    .agg({"vehicle_count": "sum"})
    .withColumnRenamed("sum(vehicle_count)", "total_vehicle_count")
)
print("Traffic Volume aggregation created.")

# Analysis Task 2: Detect Congestion Hotspots.
congestion_hotspots = (
    clean_df.filter(col("congestion_level") == "HIGH")
    .groupBy("sensor_id", window(col("timestamp"), "5 minutes"))
    .count()
    .filter(col("count") >= 3)
)
print("Congestion Hotspots in Real Time.")

# Analysis Task 3: Calculate Average Speed per Sensor with 10-minute window.
avg_speed = (
    clean_df.groupBy("sensor_id", window(col("timestamp"), "10 minutes"))
    .agg({"average_speed": "avg"})
    .withColumnRenamed("avg(average_speed)", "avg_speed")
)
print("Average Speed per Sensor with Windowing.")

# Analysis Task 4: Identify Sudden Speed Drops.
sensor_window = Window.partitionBy("sensor_id").orderBy("timestamp")
speed_df = clean_df.withColumn("prev_speed", lag("average_speed").over(sensor_window))
sudden_speed_drops = speed_df.withColumn(
    "drop_percent", (col("prev_speed") - col("average_speed")) / col("prev_speed")
).filter((col("prev_speed").isNotNull()) & (col("drop_percent") >= 0.5))
print("Sudden Speed Drops aggregation created.")


# Analysis Task 5: Find the Busiest Sensors in the Last 30 Minutes.
busiest_sensors = (
    clean_df.groupBy("sensor_id", window(col("timestamp"), "30 minutes"))
    .agg({"vehicle_count": "sum"})
    .withColumnRenamed("sum(vehicle_count)", "total_vehicle_count")
)
print("Busiest Sensors aggregation created.")

top_window = Window.partitionBy("window").orderBy(desc("total_vehicle_count"))
top_sensors = busiest_sensors.withColumn("rank", row_number().over(top_window)).filter(
    col("rank") <= 3
)
print("Busiest Sensors in the Last 30 Minutes.")


# ---------- Part 3  Data Quality Checks   -------------------
# I have Performed above please review Part 3 after part 1


# --------   Part 4: Output Storage & Visualization  ----------------

# --- Write Streaming Output ----------------------

# --- a. Traffic Volume to Kafka -----------------------
traffic_volume_output_df = traffic_volume.selectExpr(
    "sensor_id as key",
    """
    to_json(
        struct(
            sensor_id,
            date_format(window.start, 'yyyy-MM-dd HH:mm:ss') as window_start,
            date_format(window.end, 'yyyy-MM-dd HH:mm:ss') as window_end,
            total_vehicle_count,
            'traffic_volume' as analysis_type
        )
    ) as value
    """,
)

query_kafka_volume = (
    traffic_volume_output_df.writeStream.format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("topic", "traffic_analysis")
    .option("checkpointLocation", "/tmp/spark_checkpoint_traffic_volume")
    .outputMode("update")
    .start()
)
print("Kafka sink query started for Traffic Volume.")

# --- b. Average Speed to Kafka ---
avg_speed_output_df = avg_speed.selectExpr(
    "sensor_id as key",
    """
    to_json(
        struct(
            sensor_id,
            date_format(window.start, 'yyyy-MM-dd HH:mm:ss') as window_start,
            date_format(window.end, 'yyyy-MM-dd HH:mm:ss') as window_end,
            avg_speed,
            'avg_speed' as analysis_type
        )
    ) as value
    """,
)

query_kafka_avg_speed = (
    avg_speed_output_df.writeStream.format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("topic", "traffic_analysis")
    .option("checkpointLocation", "/tmp/spark_checkpoint_avg_speed")
    .outputMode("update")
    .start()
)
print("Kafka sink query started for Average Speed.")


def process_batch(batch_df, batch_id):
    print(f"\n--- Processing batch: {batch_id} ---")
    if batch_df.rdd.isEmpty():
        print(f"Batch {batch_id} is empty.")
        return
    print("Batch data before ranking:")
    batch_df.show(truncate=False)

    ranking_window = Window.partitionBy("window").orderBy(desc("total_vehicle_count"))
    ranked_df = batch_df.withColumn("rank", row_number().over(ranking_window))

    print("Ranked batch data:")
    ranked_df.show(truncate=False)

    top_df = ranked_df.filter(col("rank") <= 3)
    print("Top sensors in this batch:")
    top_df.show(truncate=False)


query_top = (
    busiest_sensors.writeStream.outputMode("update")
    .option("checkpointLocation", "/tmp/spark_checkpoint_top_sensors")
    .foreachBatch(process_batch)
    .start()
)
print("ForeachBatch query for top sensors started.")
query_top.awaitTermination()
