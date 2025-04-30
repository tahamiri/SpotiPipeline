from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Spark session without Hive support
spark = SparkSession.builder \
    .appName("ListenEventsBronzeLayer") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# Read raw data from Kafka
df_raw = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka-1:9092,kafka-2:9092,kafka-3:9092") \
    .option("subscribe", "listen_events") \
    .option("startingOffsets", "earliest") \
    .load()

# Optional: Cast Kafka message value to STRING
df_raw_value = df_raw.selectExpr("CAST(value AS STRING) as raw_value")

# Write to HDFS in raw format (Parquet)
query = df_raw_value.writeStream \
    .format("parquet") \
    .option("path", "hdfs://hadoop-namenode:8020/user/bronze/listen_events") \
    .option("checkpointLocation", "hdfs://hadoop-namenode:8020/user/bronze/checkpoints/listen_events") \
    .outputMode("append") \
    .start()

query.awaitTermination()
