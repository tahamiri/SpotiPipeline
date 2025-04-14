from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, to_timestamp, to_date
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, BooleanType, DoubleType

# Initialize Spark Session with Hive support
spark = SparkSession.builder \
    .appName("AuthEventsStream") \
    .config("spark.sql.warehouse.dir", "/user/hive/warehouse") \
    .config("hive.metastore.uris", "thrift://hive-metastore:9083") \
    .enableHiveSupport() \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# Define schema for JSON data
schema = StructType([
    StructField("ts", StringType(), True),
    StructField("sessionId", IntegerType(), True),
    StructField("level", StringType(), True),
    StructField("itemInSession", IntegerType(), True),
    StructField("city", StringType(), True),
    StructField("zip", StringType(), True),
    StructField("state", StringType(), True),
    StructField("userAgent", StringType(), True),
    StructField("lon", DoubleType(), True),
    StructField("lat", DoubleType(), True),
    StructField("userId", IntegerType(), True),
    StructField("lastName", StringType(), True),
    StructField("firstName", StringType(), True),
    StructField("gender", StringType(), True),
    StructField("registration", StringType(), True),
    StructField("success", BooleanType(), True)
])

# Read data from Kafka
df_raw = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka-1:9092,kafka-2:9092,kafka-3:9092") \
    .option("subscribe", "auth_events") \
    .option("startingOffsets", "earliest") \
    .load()

# Parse JSON payload
df_parsed = df_raw.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.*")

# Add parsed timestamp and partition column
df_transformed = df_parsed \
    .withColumn("event_time", to_timestamp((col("ts").cast("long") / 1000))) \
    .withColumn("event_date", to_date(to_timestamp((col("ts").cast("long") / 1000))))  # partition column

# Cleaned data (filter out nulls)
df_cleaned = df_transformed.filter(
    col("userId").isNotNull() &
    col("sessionId").isNotNull() &
    col("level").isNotNull() &
    col("lat").isNotNull() &
    col("lon").isNotNull() &
    col("userAgent").isNotNull() &
    col("firstName").isNotNull()
)

# Write to HDFS in Parquet format, partitioned by date
query = df_cleaned.writeStream \
    .format("parquet") \
    .option("path", "hdfs://hadoop-namenode:8020/user/hive/warehouse/auth_events") \
    .option("checkpointLocation", "hdfs://hadoop-namenode:8020/user/hive/warehouse/checkpoints/auth_events") \
    .partitionBy("event_date") \
    .outputMode("append") \
    .start()

query.awaitTermination()
