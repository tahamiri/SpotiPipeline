from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StructField, StringType, LongType, BooleanType, DoubleType

# Define the schema of your Kafka JSON messages
schema = StructType([
    StructField("ts", LongType(), True),
    StructField("sessionId", LongType(), True),
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
    StructField("registration", LongType(), True),
    StructField("success", BooleanType(), True)
])

# Create Spark session
spark = SparkSession.builder \
    .appName("AuthEventsBronzeLayer") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# Read raw data from Kafka
df_raw = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka-1:9092,kafka-2:9092,kafka-3:9092") \
    .option("subscribe", "auth_events") \
    .option("startingOffsets", "earliest") \
    .load()

# Extract and parse JSON from Kafka value
df_parsed = df_raw.selectExpr("CAST(value AS STRING) as raw_json") \
    .withColumn("data", from_json("raw_json", schema)) \
    .select("data.*")

# Write structured data to HDFS as proper Parquet
query = df_parsed.writeStream \
    .format("parquet") \
    .option("path", "hdfs://hadoop-namenode:8020/user/bronze/auth_events") \
    .option("checkpointLocation", "hdfs://hadoop-namenode:8020/user/bronze/checkpoints/auth_events") \
    .outputMode("append") \
    .start()

query.awaitTermination()
