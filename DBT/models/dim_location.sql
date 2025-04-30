WITH locations AS (
    SELECT DISTINCT city, state, zip, lat, lon
    FROM auth_events
    UNION
    SELECT DISTINCT city, state, zip, lat, lon
    FROM listen_events
    UNION
    SELECT DISTINCT city, state, zip, lat, lon
    FROM page_view_events
    UNION
    SELECT DISTINCT city, state, zip, lat, lon
    FROM status_change_events
)

SELECT 
    row_number() OVER (ORDER BY city, state, zip) AS location_id,
    city,
    state,
    zip,
    lat,
    lon
FROM locations