-- 動態日期區間
-- 另一種常碰到的狀況是以當下日期往前推N天的日期區間，這個「當下日期」應該是要隨著查詢的當下自動更新，而不是由我們手動輸入，那麼可以試著用下面的SQL語法：

SELECT
    *
FROM
    -- 更改下面的project id、dataset id、table id
   `bigquery-public-data.ga4_obfuscated_sample_ecommerce.events_*`
WHERE
    -- 更換日期區間 (過去1至7天的資料)
    _table_suffix between format_date('%Y%m%d',date_sub(current_date(), interval 7 day))
    and format_date('%Y%m%d',date_sub(current_date(), interval 1 day))
    