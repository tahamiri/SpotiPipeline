select 
    ts,
    from_unixtime(ts / 1000) as ts_ts,
    userId,
    sessionId,
    song,
    artist,
    duration,
    level
from parquet.`hdfs://hadoop-namenode:8020/user/bronze/listen_events`
where userId is not null
