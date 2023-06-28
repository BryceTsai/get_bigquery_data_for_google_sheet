with prep_sql as (
SELECT
  user_pseudo_id,
  event_timestamp,
  CASE
    WHEN page_location LIKE '%gclid%' THEN 'google'
    ELSE source_value
  END AS source,
  CASE
    WHEN page_location LIKE '%gclid%' THEN 'cpc'
    ELSE medium_value
  END AS medium
FROM (
        SELECT
            user_pseudo_id,
            event_timestamp,
            (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'page_location') AS page_location,
            (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'source') AS source_value,
            (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'medium') AS medium_value
        FROM `merkle-taiwan-training.ga4_sample_data.events_20210131`
        WHERE event_name = 'page_view' AND (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'medium') IS NOT NULL
        GROUP BY 1,2,3,4,5
    )
)

SELECT
  count(*) AS cnt,
  source,
  medium
FROM
    prep_sql
GROUP BY
    2,3
ORDER BY
    cnt desc