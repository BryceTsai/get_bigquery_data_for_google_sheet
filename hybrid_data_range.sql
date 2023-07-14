-- 靜態+動態日期區間
-- 那麼如果想要把上述的2種方式合併在一起可以嗎？例如我想要某一天的日期為起始日，並且查詢到今日之前的5天為結束日當作日期區間，那麼你可以這樣作：

SELECT
    *
FROM
    -- 更改下面的project id、dataset id、table id
   `bigquery-public-data.ga4_obfuscated_sample_ecommerce.events_*`
WHERE
    -- 更換日期區間
    _table_suffix between '20210131'
    and format_date('%Y%m%d',date_sub(current_date(), interval 5 day))