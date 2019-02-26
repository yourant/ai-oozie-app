set mapreduce.job.queuename=root.ai.offline;
CREATE TABLE IF NOT EXISTS dw_zaful_recommend.zaful_pv_sku_test(glb_oi STRING, glb_tm STRING, sku STRING);
INSERT
  OVERWRITE TABLE dw_zaful_recommend.zaful_pv_sku_test
SELECT
  *
FROM
  (
    SELECT
      glb_oi,
      glb_tm,
      GET_JSON_OBJECT(glb_ubcta_col, '$.sku') sku
    FROM
      (
        SELECT
          glb_oi,
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
  glb_oi IS NOT NULL
  AND sku IS NOT NULL;-- 点击数据
  CREATE TABLE IF NOT EXISTS dw_zaful_recommend.zaful_click_sku_test(glb_oi STRING, glb_tm STRING, sku STRING);
INSERT
  OVERWRITE TABLE dw_zaful_recommend.zaful_click_sku_test
SELECT
  *
FROM
  (
    SELECT
      glb_oi,
      glb_tm,
      GET_JSON_OBJECT(glb_skuinfo, '$.sku') sku
    FROM
      stg.zf_pc_event_info
    WHERE
      year = ${YEAR}
      AND month = ${MONTH}
      AND day = ${DAY}
      AND glb_t = 'ic'
      AND glb_skuinfo != ''
  ) a
WHERE
  glb_oi IS NOT NULL
  AND sku IS NOT NULL;--   融合数据
  CREATE TABLE IF NOT EXISTS dw_zaful_recommend.zaful_bury_sku_test(
    glb_oi STRING,
    glb_tm STRING,
    sku STRING,
    ways STRING
  );
INSERT
  OVERWRITE TABLE dw_zaful_recommend.zaful_bury_sku_test
SELECT
  glb_oi,
  glb_tm,
  sku,
  'ie' AS ways
FROM
  dw_zaful_recommend.zaful_pv_sku_test
UNION ALL
SELECT
  glb_oi,
  glb_tm,
  sku,
  'ic' AS ways
FROM
  dw_zaful_recommend.zaful_click_sku_test;
CREATE TABLE IF NOT EXISTS dw_zaful_recommend.zaful_bury_sku_regroup(
    glb_oi STRING,
    glb_tm STRING,
    sku STRING,
    ways STRING
  )PARTITIONED BY (year string, month string, day string) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' LINES TERMINATED BY '\n' STORED AS TEXTFILE;
INSERT
  OVERWRITE TABLE dw_zaful_recommend.zaful_bury_sku_regroup PARTITION(year = ${YEAR}, month = ${MONTH}, day = ${DAY})
select
  c.*
from
  (
    select
      glb_oi
    from
      (
        select
          glb_oi,
          count(1) cnt
        from
          dw_zaful_recommend.zaful_bury_sku_test
        group by
          glb_oi
      ) a
    WHERE
      cnt BETWEEN 20
      AND 1000
      AND glb_oi != ''
  ) b
  JOIN dw_zaful_recommend.zaful_bury_sku_test c on b.glb_oi = c.glb_oi;
  