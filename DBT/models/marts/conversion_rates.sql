{{
  config(materialized='table')
}}

SELECT
  userId,
  COUNT(CASE WHEN level = 'free' THEN 1 END) AS free_count,
  COUNT(CASE WHEN level = 'paid' THEN 1 END) AS paid_count,
  COUNT(CASE WHEN level = 'paid' THEN 1 END) * 1.0 /
    (COUNT(CASE WHEN level = 'free' THEN 1 END) + COUNT(CASE WHEN level = 'paid' THEN 1 END))
    AS conversion_rate
FROM {{ ref('fact_auth_events') }}
GROUP BY userId;