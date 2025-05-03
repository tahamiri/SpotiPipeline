with combined_ts as (
    select ts from parquet.`hdfs://hadoop-namenode:8020/user/bronze/auth_events`
    union
    select ts from parquet.`hdfs://hadoop-namenode:8020/user/bronze/listen_events`
    union
    select ts from parquet.`hdfs://hadoop-namenode:8020/user/bronze/page_view_events`
),
with_ts_ts as (
    select distinct ts, from_unixtime(ts / 1000) as ts_ts
    from combined_ts
)

select 
    ts,
    ts_ts,
    year(ts_ts) as year,
    month(ts_ts) as month,
    day(ts_ts) as day,
    hour(ts_ts) as hour,
    minute(ts_ts) as minute
from with_ts_ts
