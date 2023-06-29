SELECT 
    EXTRACT(DATE FROM TIMESTAMP_MICROS(event_timestamp) AT TIME ZONE "UTC+8") AS Date,
    EXTRACT(HOUR FROM TIMESTAMP_MICROS(event_timestamp) AT TIME ZONE "UTC+8") AS Hour,
    EXTRACT(MINUTE FROM TIMESTAMP_MICROS(event_timestamp) AT TIME ZONE "UTC+8") AS Minute,
    (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'page_location') AS page,
    (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'page_title') AS title
FROM    -- 更改為您的bigquery project
    `merkle-taiwan-training.ga4_sample_data.events_20201130`
WHERE event_name = 'page_view' OR event_name = 'screen_view'
ORDER BY Hour DESC, Minute