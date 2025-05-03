{{
  config(materialized='table')
}}

SELECT
  u.level,
  COUNT(f.userId) AS user_count
FROM {{ ref('fact_listen_events') }} AS f
JOIN {{ ref('dim_user') }} AS u
  ON f.userId = u.userId
GROUP BY u.level;