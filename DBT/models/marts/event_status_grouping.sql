{{
  config(materialized='table')
}}

SELECT
  status,
  COUNT(sessionId) AS event_count
FROM {{ ref('fact_page_view_events') }}
GROUP BY status;