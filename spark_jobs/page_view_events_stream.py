from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, to_timestamp, when
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, BooleanType, DoubleType, LongType

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("PageViewEventsStream") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# Define schema for page_view_events JSON data
schema = StructType([
    StructField("ts", LongType(), True),
    StructField("sessionId", IntegerType(), True),
    StructField("page", StringType(), True),
    StructField("auth", StringType(), True),
    StructField("method", StringType(), True),
    StructField("status", IntegerType(), True),
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
    StructField("registration", LongType(), True),
    StructField("artist", StringType(), True),
    StructField("song", StringType(), True),
    StructField("duration", DoubleType(), True)
])

# Read data from Kafka
df_raw = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka-1:9092,kafka-2:9092,kafka-3:9092") \
    .option("subscribe", "page_view_events") \
    .option("startingOffsets", "earliest") \
    .load()

# Convert JSON string into DataFrame
df_parsed = df_raw.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.*")

# Convert timestamp from milliseconds to readable format
df_transformed = df_parsed.withColumn("event_time", to_timestamp(col("ts") / 1000))

# Data cleaning and validation
df_cleaned = df_transformed \
    .filter(
        (col("userId").isNotNull()) &
        (col("sessionId").isNotNull()) &
        (col("page").isNotNull()) &
        (col("level").isNotNull()) &
        (col("lat").isNotNull()) &
        (col("lon").isNotNull()) &
        (col("userAgent").isNotNull()) &
        (col("firstName").isNotNull()) &
        (col("lastName").isNotNull()) &
        (col("ts").isNotNull())
    ) \
    .filter(
        (col("lat") >= -90) & (col("lat") <= 90) &  # Latitude range check
        (col("lon") >= -180) & (col("lon") <= 180) &  # Longitude range check
        (col("userId") > 0) &  # userId validation
        (col("sessionId") > 0) &  # sessionId validation
        (col("itemInSession") >= 0)  # Relax itemInSession validation to allow 0
    ) \
    .withColumn(
        "userAgent", 
        when(col("userAgent").rlike(r'[^\x00-\x7F]'), None).otherwise(col("userAgent"))
    )


# Write output to console (or any sink)
query = df_cleaned.writeStream \
    .outputMode("update") \
    .format("console") \
    .trigger(processingTime="1 minute") \
    .start()

query.awaitTermination()
