from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, window, avg, count, expr
from pyspark.sql.types import StructType, StructField, IntegerType, FloatType, StringType

spark = SparkSession.builder \
    .appName("Mini Project 3 - Streaming Recommender") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

schema = StructType([
    StructField("user_id", IntegerType(), True),
    StructField("item_id", IntegerType(), True),
    StructField("rating", FloatType(), True),
    StructField("timestamp", StringType(), True)
])

raw_stream = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "user_interactions") \
    .option("startingOffsets", "latest") \
    .load()

parsed = raw_stream.selectExpr("CAST(value AS STRING) AS json_value") \
    .select(from_json(col("json_value"), schema).alias("data")) \
    .select("data.*") \
    .dropna()

events = parsed.withColumn("event_time", col("timestamp").cast("timestamp"))

window_metrics = events.withWatermark("event_time", "1 minute") \
    .groupBy(
        window(col("event_time"), "30 seconds", "10 seconds"),
        col("item_id")
    ) \
    .agg(
        avg("rating").alias("avg_rating"),
        count("*").alias("interaction_count")
    ) \
    .withColumn("trending_score", col("avg_rating") * col("interaction_count"))

alerts = window_metrics.filter(
    (col("avg_rating") > 4.5) | (col("interaction_count") > 10)
).withColumn(
    "alert_message",
    expr("concat('ALERT: Item ', item_id, ' is trending')")
)

query_metrics = window_metrics.writeStream \
    .outputMode("update") \
    .format("console") \
    .option("truncate", False) \
    .start()

query_alerts = alerts.writeStream \
    .outputMode("update") \
    .format("console") \
    .option("truncate", False) \
    .start()

query_metrics.awaitTermination()
query_alerts.awaitTermination()
