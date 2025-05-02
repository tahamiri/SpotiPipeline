from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json
from pyspark.sql.types import StructType, StructField, StringType, LongType, DoubleType

# Define schema for page view events
page_view_schema = StructType([
    StructField("ts", LongType(), True),
    StructField("sessionId", LongType(), True),
    StructField("page", StringType(), True),
    StructField("auth", StringType(), True),
    StructField("method", StringType(), True),
    StructField("status", LongType(), True),
    StructField("level", StringType(), True),
    StructField("itemInSession", LongType(), True),
    StructField("city", StringType(), True),
    StructField("zip", StringType(), True),
    StructField("state", StringType(), True),
    StructField("userAgent", StringType(), True),
    StructField("lon", DoubleType(), True),
    StructField("lat", DoubleType(), True),
    StructField("userId", LongType(), True),
    StructField("lastName", StringType(), True),
    StructField("firstName", StringType(), True),
    StructField("gender", StringType(), True),
    StructField("registration", LongType(), True)
])

# Create Spark session
spark = SparkSession.builder \
    .appName("PageViewEventsBronzeLayer") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# Read from Kafka topic: page_view_events
df_raw = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka-1:9092,kafka-2:9092,kafka-3:9092") \
    .option("subscribe", "page_view_events") \
    .option("startingOffsets", "earliest") \
    .load()

# Parse the raw Kafka value (JSON)
df_parsed = df_raw.selectExpr("CAST(value AS STRING) AS raw_json") \
    .withColumn("data", from_json("raw_json", page_view_schema)) \
    .select("data.*")

# Write to HDFS in Parquet format
query = df_parsed.writeStream \
    .format("parquet") \
    .option("path", "hdfs://hadoop-namenode:8020/user/bronze/page_view_events") \
    .option("checkpointLocation", "hdfs://hadoop-namenode:8020/user/bronze/checkpoints/page_view_events") \
    .outputMode("append") \
    .start()

query.awaitTermination()
