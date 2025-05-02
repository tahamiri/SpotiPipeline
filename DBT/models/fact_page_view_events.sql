select 
    ts,
    from_unixtime(ts / 1000) as ts_ts,
    userId,
    sessionId,
    page,
    method,
    status,
    level
from parquet.`hdfs://hadoop-namenode:8020/user/bronze/page_view_events`
where userId is not null
