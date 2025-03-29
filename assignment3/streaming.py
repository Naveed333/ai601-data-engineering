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

# 1) Create SparkSession.
spark = SparkSession.builder.appName("TrafficMonitoring").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

# 2) Define the schema for traffic events.
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

# 3) Read streaming data from Kafka topic "traffic_data".
kafka_df = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("subscribe", "traffic_data")
    .option("startingOffsets", "latest")
    .option("failOnDataLoss", "false")
    .load()
)
print("Kafka stream created.")

# 4) Convert the binary 'value' to string and parse JSON.
json_df = kafka_df.selectExpr("CAST(value AS STRING) as json_str")
parsed_df = json_df.select(from_json(col("json_str"), schema).alias("data"))
# Flatten the nested structure.
events_df = parsed_df.select("data.*")
print("Parsed events_df schema:")
events_df.printSchema()

# 5) Data Quality Checks.
# Filter on the flattened DataFrame (events_df).
clean_df = events_df.filter(col("sensor_id").isNotNull() & col("timestamp").isNotNull())
clean_df = clean_df.filter((col("vehicle_count") >= 0) & (col("average_speed") > 0))
clean_df = clean_df.dropDuplicates(["sensor_id", "timestamp"])
print("Cleaned data (logical plan):")
clean_df.printSchema()

# 6) Convert timestamp string to actual TimestampType.
clean_df = clean_df.withColumn("timestamp", col("timestamp").cast(TimestampType()))

# --- Analytics ---

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
print("Congestion Hotspots aggregation created.")

# Analysis Task 3: Calculate Average Speed per Sensor with 10-minute window.
avg_speed = (
    clean_df.groupBy("sensor_id", window(col("timestamp"), "10 minutes"))
    .agg({"average_speed": "avg"})
    .withColumnRenamed("avg(average_speed)", "avg_speed")
)
print("Average Speed aggregation created.")

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

# Rank sensors and get top 3 within each window.
top_window = Window.partitionBy("window").orderBy(desc("total_vehicle_count"))
top_sensors = busiest_sensors.withColumn("rank", row_number().over(top_window)).filter(
    col("rank") <= 3
)
print("Top sensors (ranking) transformation created.")

# --- Write Streaming Output ---

# (a) Write the aggregated traffic volume back to Kafka topic "traffic_analysis".
output_df = traffic_volume.selectExpr(
    "to_json(struct(sensor_id, window, total_vehicle_count)) as value"
)
query_kafka = (
    output_df.writeStream.format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("topic", "traffic_analysis")
    .option("checkpointLocation", "/tmp/spark_checkpoint_traffic_volume")
    .outputMode("update")
    .start()
)
print("Kafka sink query started.")

# (b) Write the aggregated traffic volume to the console.
query_console = (
    traffic_volume.writeStream.format("console")
    .option("truncate", "false")
    .outputMode("update")
    .start()
)
print("Console sink query started.")


# (c) Use foreachBatch to print the top sensors per micro-batch.
def process_batch(batch_df, batch_id):
    print(f"\n--- Processing batch: {batch_id} ---")
    if batch_df.rdd.isEmpty():
        print(f"Batch {batch_id} is empty.")
        return
    print("Batch data before ranking:")
    batch_df.show(truncate=False)

    # Apply window function on the batch DataFrame for ranking.
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

# Wait for any termination (all queries run concurrently).
query_top.awaitTermination()
