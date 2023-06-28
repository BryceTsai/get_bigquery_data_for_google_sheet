SELECT
    -- date (dimension)
    event_date as date,
    -- year (dimension)
    format_date('%Y',parse_date("%Y%m%d",event_date)) as year,
    -- iso year (dimension)
    format_date('%G',parse_date("%Y%m%d",event_date)) as iso_year,
    -- month of year (dimension)
    format_date('%Y%m',parse_date("%Y%m%d",event_date)) as month_of_year,
    -- month of the year (dimension)
    format_date('%m',parse_date("%Y%m%d",event_date)) as month_of_the_year,
    -- week of year (dimension)
    format_date('%Y%U',parse_date("%Y%m%d",event_date)) as week_of_year,
    -- week of the year (dimension)
    format_date('%U',parse_date("%Y%m%d",event_date)) as week_of_the_year,
    -- iso week of the year (dimension)
    format_date('%W',parse_date("%Y%m%d",event_date)) as iso_week_of_the_year,
    -- iso week of iso year (dimension)
    format_date('%G%W',parse_date("%Y%m%d",event_date)) as iso_week_of_iso_year,
    -- day of the month (dimension)
    format_date('%d',parse_date("%Y%m%d",event_date)) as day_of_the_month,
    -- day of week (dimension)
    format_date('%w',parse_date("%Y%m%d",event_date)) as day_of_week,
    -- day of week name (dimension)
    format_date('%A',parse_date("%Y%m%d",event_date)) as day_of_week_name,
    -- hour of day(dimension)
    concat(event_date,cast(format("%02d",extract(hour from timestamp_micros(event_timestamp))) as string)) as hour_of_day,
    -- hour (dimension)
    format("%02d",extract(hour from timestamp_micros(event_timestamp))) as hour,
    -- minute (dimension)
    format("%02d",extract(minute from timestamp_micros(event_timestamp))) as minute,
    -- date hour and minute (dimension)
    concat(concat(event_date,cast(format("%02d",extract(hour from timestamp_micros(event_timestamp))) as string)),format("%02d",extract(minute from timestamp_micros(event_timestamp)))) as date_hour_and_minute
FROM    -- 更改為您的bigquery project
    `merkle-taiwan-training.ga4_sample_data.events_20201130`
WHERE event_name = 'page_view' OR event_name = 'screen_view'
ORDER BY Hour, Minute