with listen_events as (
    select * from parquet.`hdfs://hadoop-namenode:8020/user/bronze/listen_events`
    where userId is not null
),
listen_with_ts as (
    select *,
           from_unixtime(ts / 1000) as ts_ts
    from listen_events
)

select distinct song, artist, duration
from listen_with_ts
