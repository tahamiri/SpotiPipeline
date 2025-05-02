from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, from_unixtime, to_timestamp, year, month, dayofmonth, hour, minute,
    concat_ws
)
from pyspark.sql.types import *

spark = SparkSession.builder.appName("SilverLayer").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

# ========== Define Schemas ==========
auth_schema = StructType([
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

page_schema = StructType([
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

# ========== Read Bronze Streams ==========
base = "hdfs://hadoop-namenode:8020/user/bronze"

auth_df = spark.readStream.schema(auth_schema).parquet(f"{base}/auth_events")
listen_df = spark.readStream.schema(listen_schema).parquet(f"{base}/listen_events")
page_df = spark.readStream.schema(page_schema).parquet(f"{base}/page_view_events")

# ========== Common Processing ==========
def add_ts_ts(df):
    return df.withColumn("ts_ts", to_timestamp(from_unixtime((col("ts") / 1000).cast("long"))))

auth_df = add_ts_ts(auth_df).filter(col("userId").isNotNull())
listen_df = add_ts_ts(listen_df).filter(col("userId").isNotNull())
page_df = add_ts_ts(page_df).filter(col("userId").isNotNull())

# ========== Build Dimension Tables ==========
dim_user = auth_df.select(
    "userId", "firstName", "lastName", "gender", "level", "registration"
).dropDuplicates(["userId"])

dim_song = listen_df.select("song", "artist", "duration").dropDuplicates(["song", "artist"])

dim_location = auth_df.select("city", "state", "zip", "lon", "lat").dropDuplicates(["city", "state", "zip"])

time_cols = {
    "year": year("ts_ts"),
    "month": month("ts_ts"),
    "day": dayofmonth("ts_ts"),
    "hour": hour("ts_ts"),
    "minute": minute("ts_ts")
}
dim_time = auth_df.select("ts", "ts_ts").union(listen_df.select("ts", "ts_ts")).union(page_df.select("ts", "ts_ts")) \
    .dropDuplicates(["ts"]).select("ts", "ts_ts", *[v.alias(k) for k, v in time_cols.items()])

# ========== Fact Tables ==========
fact_auth = auth_df.select("ts", "ts_ts", "userId", "sessionId", "level", "success")

fact_listen = listen_df.select(
    "ts", "ts_ts", "userId", "sessionId", "song", "artist", "duration", "level"
)

fact_page = page_df.select(
    "ts", "ts_ts", "userId", "sessionId", "page", "method", "status", "level"
)

# ========== Write Streams ==========
def write_stream(df, name):
    df.writeStream \
        .format("parquet") \
        .option("path", f"hdfs://hadoop-namenode:8020/user/silver/{name}") \
        .option("checkpointLocation", f"hdfs://hadoop-namenode:8020/user/silver/checkpoints/{name}") \
        .outputMode("append") \
        .start()

# Write dimensions
write_stream(dim_user, "dim_user")
write_stream(dim_song, "dim_song")
write_stream(dim_location, "dim_location")
write_stream(dim_time, "dim_time")

# Write facts
write_stream(fact_auth, "fact_auth_events")
write_stream(fact_listen, "fact_listen_events")
write_stream(fact_page, "fact_page_view_events")

# Block until termination
spark.streams.awaitAnyTermination()
