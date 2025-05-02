select 
    ts,
    from_unixtime(ts / 1000) as ts_ts,
    userId,
    sessionId,
    level,
    success
from parquet.`hdfs://hadoop-namenode:8020/user/bronze/auth_events`
where userId is not null
