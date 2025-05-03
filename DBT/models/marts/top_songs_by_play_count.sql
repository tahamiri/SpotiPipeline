
{{
  config(materialized='table')
}}

SELECT
  song,
  COUNT(sessionId) AS play_count
FROM {{ ref('fact_listen_events') }}
GROUP BY song;