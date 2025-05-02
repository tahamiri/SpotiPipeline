from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StructField, StringType, LongType, DoubleType

# Define the schema for listen_events
listen_schema = StructType([
    StructField("artist", StringType(), True),
    StructField("song", StringType(), True),
    StructField("duration", DoubleType(), True),
    StructField("ts", LongType(), True),
    StructField("sessionId", LongType(), True),
    StructField("auth", StringType(), True),
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
    .appName("ListenEventsBronzeLayer") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# Read from Kafka
df_raw = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka-1:9092,kafka-2:9092,kafka-3:9092") \
    .option("subscribe", "listen_events") \
    .option("startingOffsets", "earliest") \
    .load()

# Extract and parse JSON
df_parsed = df_raw.selectExpr("CAST(value AS STRING) as raw_json") \
    .withColumn("data", from_json("raw_json", listen_schema)) \
    .select("data.*")

# Write structured data to HDFS as Parquet
query = df_parsed.writeStream \
    .format("parquet") \
    .option("path", "hdfs://hadoop-namenode:8020/user/bronze/listen_events") \
    .option("checkpointLocation", "hdfs://hadoop-namenode:8020/user/bronze/checkpoints/listen_events") \
    .outputMode("append") \
    .start()

query.awaitTermination()
