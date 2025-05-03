from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, avg, to_date, when, weekofyear
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType
from pyspark.sql.types import LongType ,IntegerType, BooleanType

# Initialize Spark session
spark = SparkSession.builder \
    .appName("GoldLayerBatchJob") \
    .getOrCreate()

# Define schemas
dim_location_schema = StructType([
    StructField("city", StringType(), True),
    StructField("state", StringType(), True),
    StructField("zip", StringType(), True),
    StructField("lon", DoubleType(), True),
    StructField("lat", DoubleType(), True),
])

dim_song_schema = StructType([
    StructField("song", StringType(), True),
    StructField("artist", StringType(), True),
    StructField("duration", DoubleType(), True),
])

dim_time_schema = StructType([
    StructField("ts", StringType(), True),
    StructField("ts_ts", TimestampType(), True),
    StructField("year", IntegerType(), True),
    StructField("month", IntegerType(), True),
    StructField("day", IntegerType(), True),
    StructField("hour", IntegerType(), True),
    StructField("minute", IntegerType(), True),
])

dim_user_schema = StructType([
    StructField("userId", LongType(), True),
    StructField("firstName", StringType(), True),
    StructField("lastName", StringType(), True),
    StructField("gender", StringType(), True),
    StructField("level", StringType(), True),
    StructField("registration", LongType(), True),
])


fact_auth_events_schema = StructType([
    StructField("ts", StringType(), True),
    StructField("ts_ts", TimestampType(), True),
    StructField("userId", LongType(), True),
    StructField("sessionId", LongType(), True),
    StructField("level", StringType(), True),
    StructField("success", BooleanType(), True),  # changed from StringType() to BooleanType()
])

fact_listen_events_schema = StructType([
    StructField("ts", StringType(), True),
    StructField("ts_ts", TimestampType(), True),
    StructField("userId", LongType(), True),
    StructField("sessionId", LongType(), True),
    StructField("song", StringType(), True),
    StructField("artist", StringType(), True),
    StructField("duration", DoubleType(), True),
    StructField("level", StringType(), True),
])

fact_page_view_events_schema = StructType([
    StructField("ts", StringType(), True),
    StructField("ts_ts", TimestampType(), True),
    StructField("userId", LongType(), True),
    StructField("sessionId", LongType(), True),
    StructField("page", StringType(), True),
    StructField("method", StringType(), True),
    StructField("status", LongType(), True), 
    StructField("level", StringType(), True),
])

# Paths
silver_path = "hdfs://hadoop-namenode:8020/user/silver/"
gold_path = "hdfs://hadoop-namenode:8020/user/gold/"

# Read batch data
fact_listen_events = spark.read.schema(fact_listen_events_schema).parquet(silver_path + "fact_listen_events")
fact_auth_events = spark.read.schema(fact_auth_events_schema).parquet(silver_path + "fact_auth_events")
fact_page_view_events = spark.read.schema(fact_page_view_events_schema).parquet(silver_path + "fact_page_view_events")
dim_user = spark.read.schema(dim_user_schema).parquet(silver_path + "dim_user")
dim_song = spark.read.schema(dim_song_schema).parquet(silver_path + "dim_song")
dim_location = spark.read.schema(dim_location_schema).parquet(silver_path + "dim_location")
dim_time = spark.read.schema(dim_time_schema).parquet(silver_path + "dim_time")

# 1. Daily user listen counts
daily_user_listen_counts = fact_listen_events \
    .join(dim_time.drop("ts"), "ts_ts") \
    .groupBy("userId", "year", "month", "day") \
    .agg(count("sessionId").alias("listen_count"))

# 2. Weekday mean listening time
weekday_mean_listening_time = fact_listen_events \
    .withColumn("weekday", weekofyear(col("ts_ts"))) \
    .groupBy("weekday") \
    .agg(avg("duration").alias("mean_listening_time"))

# 3. Top songs by play count
top_songs_by_play_count = fact_listen_events \
    .groupBy("song") \
    .agg(count("sessionId").alias("play_count"))

# 4. Free vs paid users comparison
free_vs_paid_users_comparison = fact_listen_events.alias("f") \
    .join(dim_user.alias("u"), "userId") \
    .groupBy("u.level") \
    .agg(count("f.userId").alias("user_count"))

# 5. Mean session time (now correctly using integer success values)

mean_session_time = fact_auth_events \
    .withColumn("success_int", col("success").cast("int")) \
    .groupBy("userId") \
    .agg(avg("success_int").alias("avg_session_time"))

# 6. Conversion rates
conversion_rates = fact_auth_events \
    .groupBy("userId") \
    .agg(
        count(when(col("level") == "free", True)).alias("free_count"),
        count(when(col("level") == "paid", True)).alias("paid_count")
    ) \
    .withColumn("conversion_rate", col("paid_count") / (col("free_count") + col("paid_count")))

# 7. Event status grouping
event_status_grouping = fact_page_view_events \
    .groupBy("status") \
    .agg(count("sessionId").alias("event_count"))

# 8. New user week 1 behavior
new_user_week1_behavior = fact_auth_events.alias("f") \
    .join(dim_user.alias("u"), "userId") \
    .withColumn("days_since_registration", (col("f.ts_ts").cast("long") - col("u.registration").cast("long")) / (60 * 60 * 24)) \
    .filter(col("days_since_registration") <= 7) \
    .groupBy("userId") \
    .agg(count("f.sessionId").alias("new_user_behavior"))

# Write outputs to gold layer (as batch)
daily_user_listen_counts.write.mode("overwrite").parquet(gold_path + "daily_user_listen_counts")
weekday_mean_listening_time.write.mode("overwrite").parquet(gold_path + "weekday_mean_listening_time")
top_songs_by_play_count.write.mode("overwrite").parquet(gold_path + "top_songs_by_play_count")
free_vs_paid_users_comparison.write.mode("overwrite").parquet(gold_path + "free_vs_paid_users_comparison")
mean_session_time.write.mode("overwrite").parquet(gold_path + "mean_session_time")
conversion_rates.write.mode("overwrite").parquet(gold_path + "conversion_rates")
event_status_grouping.write.mode("overwrite").parquet(gold_path + "event_status_grouping")
new_user_week1_behavior.write.mode("overwrite").parquet(gold_path + "new_user_week1_behavior")
