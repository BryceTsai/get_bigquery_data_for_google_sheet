with prep_sql as (
SELECT
  user_pseudo_id,
  event_timestamp,
  event_date,
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
            event_date,
            (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'page_location') AS page_location,
            (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'source') AS source_value,
            (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'medium') AS medium_value
            --更改下面的project id、dataset id、table id
        FROM `merkle-taiwan-training.ga4_sample_data.events_20210131`
        WHERE event_name = 'page_view' AND (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'medium') IS NOT NULL
        GROUP BY 1,2,3,4,5,6
    )
)

SELECT
  event_date,
  source,
  medium,
  count(*) AS cnt
FROM
    prep_sql
GROUP BY
    1,2,3
ORDER BY
    cnt desc