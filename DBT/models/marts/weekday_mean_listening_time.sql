{{
  config(materialized='table')
}}

SELECT
  WEEKOFYEAR(ts_ts) AS weekday,
  AVG(duration) AS mean_listening_time
FROM {{ ref('fact_listen_events') }}
GROUP BY WEEKOFYEAR(ts_ts);