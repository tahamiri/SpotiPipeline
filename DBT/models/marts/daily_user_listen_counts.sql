
{{
  config(materialized='table')
}}

SELECT
  fle.userId,
  dt.year,
  dt.month,
  dt.day,
  COUNT(fle.sessionId) AS listen_count
FROM {{ ref('fact_listen_events') }} AS fle
JOIN {{ ref('dim_time') }} AS dt
  ON fle.ts_ts = dt.ts_ts
GROUP BY fle.userId, dt.year, dt.month, dt.day;