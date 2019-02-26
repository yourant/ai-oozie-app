set mapreduce.job.queuename = root.ai.offline;
set hive.support.concurrency=false;
CREATE TABLE IF NOT EXISTS dw_zaful_recommend.zaful_pv_sku_listpagetest_3days(glb_od STRING, sku STRING);
INSERT
  OVERWRITE TABLE dw_zaful_recommend.zaful_pv_sku_listpagetest_3days
SELECT
  glb_od,
  concat(sku, '#', glb_tm) new_sku
FROM
  (
    SELECT
      glb_od,
      GET_JSON_OBJECT(glb_ubcta_col, '$.sku') sku,
      glb_tm
    FROM
      (
        SELECT
          glb_od,
          log_id,
          glb_tm
        FROM
          stg.zf_pc_event_info
        WHERE
          year = ${YEAR}
          AND month = ${MONTH}
          AND day = ${DAY}
          AND glb_t = 'ie'
          AND glb_ubcta != ''
      ) a
      JOIN (
        SELECT
          log_id,
          glb_ubcta_col
        FROM
          stg.zf_pc_event_ubcta_info
        WHERE
          year = ${YEAR}
          AND month = ${MONTH}
          AND day = ${DAY}
      ) b ON a.log_id = b.log_id
  ) c
WHERE
  glb_od IS NOT NULL
  AND sku IS NOT NULL;-- 点击数据
  CREATE TABLE IF NOT EXISTS dw_zaful_recommend.zaful_click_sku_listpagetest_3days(glb_od STRING, sku STRING);
INSERT
  OVERWRITE TABLE dw_zaful_recommend.zaful_click_sku_listpagetest_3days
SELECT
  glb_od,
  concat(sku, '#', glb_tm) new_sku
FROM
  (
    SELECT
      glb_od,
      GET_JSON_OBJECT(glb_skuinfo, '$.sku') sku,
      glb_tm
    FROM
      stg.zf_pc_event_info
    WHERE
      year = ${YEAR}
      AND month = ${MONTH}
      AND day = ${DAY}
      AND glb_t = 'ic'
      AND glb_x in ('sku', 'addtobag')
      AND glb_skuinfo != ''
  ) a
WHERE
  glb_od IS NOT NULL
  AND sku IS NOT NULL;
CREATE TABLE IF NOT EXISTS dw_zaful_recommend.zaful_adt_sku_listpagetest_3days(glb_od STRING, sku STRING);
INSERT
  OVERWRITE TABLE dw_zaful_recommend.zaful_adt_sku_listpagetest_3days
SELECT
  glb_od,
  concat(sku, '#', glb_tm) new_sku
FROM
  (
    SELECT
      glb_od,
      GET_JSON_OBJECT(glb_skuinfo, '$.sku') sku,
      glb_tm
    FROM
      stg.zf_pc_event_info
    WHERE
      year = ${YEAR}
      AND month = ${MONTH}
      AND day = ${DAY}
      AND glb_t = 'ic'
      AND glb_x = 'ADT'
      AND glb_skuinfo != ''
  ) a
WHERE
  glb_od IS NOT NULL
  AND sku IS NOT NULL;--   融合数据
  CREATE TABLE IF NOT EXISTS dw_zaful_recommend.zaful_bury_sku_listpagetest_3days(glb_od STRING, sku STRING, ways STRING);
INSERT
  OVERWRITE TABLE dw_zaful_recommend.zaful_bury_sku_listpagetest_3days
SELECT
  glb_od,
  sku,
  'ie' AS ways
FROM
  dw_zaful_recommend.zaful_pv_sku_listpagetest_3days
UNION ALL
SELECT
  glb_od,
  sku,
  'ic' AS ways
FROM
  dw_zaful_recommend.zaful_click_sku_listpagetest_3days
UNION ALL
SELECT
  glb_od,
  sku,
  'adt' AS ways
FROM
  dw_zaful_recommend.zaful_adt_sku_listpagetest_3days;--过滤session会话
  CREATE TABLE IF NOT EXISTS dw_zaful_recommend.zaful_bury_sku_filter_listpagetest_3days(glb_od STRING, sku STRING, ways STRING);
INSERT
  OVERWRITE TABLE dw_zaful_recommend.zaful_bury_sku_filter_listpagetest_3days
select
  c.*
from
  (
    select
      glb_od
    from
      (
        select
          glb_od,
          count(1) cnt
        from
          dw_zaful_recommend.zaful_bury_sku_listpagetest_3days
        group by
          glb_od
      ) a
    WHERE
      cnt BETWEEN 20
      AND 1000
      AND glb_od != ''
  ) b
  JOIN dw_zaful_recommend.zaful_bury_sku_listpagetest_3days c on b.glb_od = c.glb_od;--以session会话为单位聚合操作
CREATE TABLE IF NOT EXISTS dw_zaful_recommend.zaful_word2vec(glb_od STRING, ways STRING, skustr STRING) 
  PARTITIONED BY (year string, month string, day string) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' LINES TERMINATED BY '\n' STORED AS TEXTFILE;
INSERT
  OVERWRITE TABLE dw_zaful_recommend.zaful_word2vec PARTITION(year = ${YEAR}, month = ${MONTH}, day = ${DAY})
select
  glb_od,
  ways,
  concat_ws(',', collect_set(sku)) as skustr
from
  dw_zaful_recommend.zaful_bury_sku_filter_listpagetest_3days
group by
  glb_od,
  ways;