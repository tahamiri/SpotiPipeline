with auth_events as (
    select * from parquet.`hdfs://hadoop-namenode:8020/user/bronze/auth_events`
    where userId is not null
),
auth_with_ts as (
    select *,
           from_unixtime(ts / 1000) as ts_ts
    from auth_events
)

select distinct city, state, zip, lon, lat
from auth_with_ts
