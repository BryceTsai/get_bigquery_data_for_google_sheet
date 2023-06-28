SELECT
  ROW_NUMBER() OVER (PARTITION BY user_pseudo_id ORDER BY user_pseudo_id, event_timestamp) AS user_event_number,
  *
FROM    -- 更改為您的bigquery project
    `merkle-taiwan-training.ga4_sample_data.events_20201130`
WHERE
  event_name NOT IN ("user_engagement", "session_start")
ORDER BY user_pseudo_id,event_timestamp