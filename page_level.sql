select
    case when split(split((select value.string_value from unnest(event_params) where event_name = 'page_view' and key = 'page_location'),'/')[safe_ordinal(4)],'?')[safe_ordinal(1)] = '' then null else concat('/',split(split((select value.string_value from unnest(event_params) where event_name = 'page_view' and key = 'page_location'),'/')[safe_ordinal(4)],'?')[safe_ordinal(1)]) end as pagepath_level_1,
    case when split(split((select value.string_value from unnest(event_params) where event_name = 'page_view' and key = 'page_location'),'/')[safe_ordinal(5)],'?')[safe_ordinal(1)] = '' then null else concat('/',split(split((select value.string_value from unnest(event_params) where event_name = 'page_view' and key = 'page_location'),'/')[safe_ordinal(5)],'?')[safe_ordinal(1)]) end as pagepath_level_2,
    case when split(split((select value.string_value from unnest(event_params) where event_name = 'page_view' and key = 'page_location'),'/')[safe_ordinal(6)],'?')[safe_ordinal(1)] = '' then null else concat('/',split(split((select value.string_value from unnest(event_params) where event_name = 'page_view' and key = 'page_location'),'/')[safe_ordinal(6)],'?')[safe_ordinal(1)]) end as pagepath_level_3,
        countif(event_name = 'page_view') as page_views
from
    -- 更改為您的bigquery project
    `merkle-taiwan-training.ga4_sample_data.events_*`
where
    -- 設定查詢日期
    _table_suffix between '20201130'
    and format_date('%Y%m%d',date_sub(current_date(), interval 1 day))
group by
    pagepath_level_1,
    pagepath_level_2,
    pagepath_level_3
order by
    page_views desc