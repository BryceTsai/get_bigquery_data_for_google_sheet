
SELECT 
TIMESTAMP_DIFF( TIMESTAMP_MICROS(nextPageViewTime) , TIMESTAMP_MICROS(event_timestamp) , second) as stay_time,
*
FROM (

  SELECT
    ROW_NUMBER() OVER (PARTITION BY user_pseudo_id ORDER BY user_pseudo_id, event_timestamp) AS pageVisitNumber,
    LEAD(event_timestamp, 1) OVER (PARTITION BY user_pseudo_id ORDER BY event_timestamp ASC) AS nextPageViewTime,
    *
FROM    -- 更改為您的bigquery project
    `merkle-taiwan-training.ga4_sample_data.events_20201130`
WHERE
    event_name NOT IN ("user_engagement", "session_start")
)