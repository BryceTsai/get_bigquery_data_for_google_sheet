-- 靜態日期區間
-- 如果要查詢一段含包起始日、結束日之間的資料表的話，則可以改成下面這樣：

SELECT
   *
FROM
    -- 更改下面的project id、dataset id、table id
   `bigquery-public-data.ga4_obfuscated_sample_ecommerce.events_*`
WHERE
    -- 更換日期區間
    _table_suffix between '20210101' and '20210131'