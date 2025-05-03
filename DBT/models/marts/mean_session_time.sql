{{
  config(materialized='table')
}}

SELECT
  userId,
  AVG(CAST(success AS INT)) AS avg_session_time
FROM {{ ref('fact_auth_events') }}
GROUP BY userId;