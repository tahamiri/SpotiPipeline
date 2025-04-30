from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Spark session without Hive support
spark = SparkSession.builder \
    .appName("AuthEventsBronzeLayer") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# Read raw data from Kafka
df_raw = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092,localhost:9092,localhost:9092") \
    .option("subscribe", "auth_events") \
    .option("startingOffsets", "earliest") \
    .load()

# Optional: Cast Kafka message value to STRING
df_raw_value = df_raw.selectExpr("CAST(value AS STRING) as raw_value")

# Write to HDFS in raw format (Parquet)
query = df_raw_value.writeStream \
    .format("parquet") \
    .option("path", "hdfs://localhost:8020/user/bronze/auth_events") \
    .option("checkpointLocation", "hdfs://localhost:8020/user/bronze/checkpoints/auth_events") \
    .outputMode("append") \
    .start()

query.awaitTermination()
