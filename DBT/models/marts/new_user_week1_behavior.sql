
{{
  config(materialized='table')
}}

SELECT
  f.userId,
  COUNT(f.sessionId) AS new_user_behavior
FROM {{ ref('fact_auth_events') }} AS f
JOIN {{ ref('dim_user') }} AS u
  ON f.userId = u.userId
WHERE (CAST(f.ts_ts AS BIGINT) - CAST(u.registration AS BIGINT)) / (60 * 60 * 24) <= 7
GROUP BY f.userId;