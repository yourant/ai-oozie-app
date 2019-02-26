--一级分类-末级分类统计
set mapreduce.job.queuename=root.ai.offline;
SET mapred.job.name='goods_discount_table';
SET mapred.max.split.size=128000000;
SET mapred.min.split.size=128000000;
SET mapred.min.split.size.per.node=128000000;
SET mapred.min.split.size.per.rack=128000000;
SET hive.exec.reducers.bytes.per.reducer = 128000000;
SET hive.merge.mapfiles=true;
SET hive.merge.mapredfiles= true;
SET hive.input.format=org.apache.hadoop.hive.ql.io.CombineHiveInputFormat;
SET hive.merge.size.per.task=256000000;
SET hive.exec.parallel = true; 
USE dw_zaful_recommend;


CREATE TABLE IF NOT EXISTS dw_zaful_recommend.zaful_cate(
  cat_id INT,
  cat_name STRING,
  level INT,
  node1 STRING,
  node2 STRING,
  node3 STRING,
  node4 STRING,
  node5 STRING
);
INSERT OVERWRITE TABLE dw_zaful_recommend.zaful_cate
SELECT
  cat_id,
  cat_name,
  level,
  nodes [0],
  CASE WHEN SIZE(nodes) > 1 THEN nodes [1] ELSE NULL END,
  CASE WHEN SIZE(nodes) > 2 THEN nodes [2] ELSE NULL END,
  CASE WHEN SIZE(nodes) > 3 THEN nodes [3] ELSE NULL END,
  CASE WHEN SIZE(nodes) > 4 THEN nodes [4] ELSE NULL END
FROM
  (
    SELECT
      cat_id,
      cat_name,
      level,
      SPLIT(node, ',') nodes
    FROM
      stg.zaful_eload_category
  ) a;
CREATE TABLE IF NOT EXISTS dw_zaful_recommend.zaful_sku_cate_to_node3(
    goods_sn STRING,
    node1 STRING,
    node2 STRING,
    node3 STRING
  );
INSERT OVERWRITE TABLE dw_zaful_recommend.zaful_sku_cate_to_node3
SELECT
  a.goods_sn,
  b.node1,
  b.node2,
  b.node3
FROM
  stg.zaful_eload_goods a
  JOIN dw_zaful_recommend.zaful_cate b ON a.cat_id = b.cat_id;
-- 曝光数据
CREATE TABLE IF NOT EXISTS dw_zaful_recommend.zaful_pv_sku(glb_oi STRING,sku STRING);
INSERT OVERWRITE TABLE dw_zaful_recommend.zaful_pv_sku
SELECT
  *
FROM
  (
    SELECT
      glb_oi,
      GET_JSON_OBJECT(glb_ubcta_col, '$.sku') sku
    FROM
      (
        SELECT
          glb_oi,
          log_id
        FROM
          stg.zf_pc_event_info
        WHERE
          CONCAT(year, month, day) BETWEEN ${DATE}
          AND ${ADD_TIME_W}
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
          CONCAT(year, month, day) BETWEEN ${DATE}
          AND ${ADD_TIME_W}
      ) b ON a.log_id = b.log_id
  ) c
WHERE
  glb_oi IS NOT NULL
  AND sku IS NOT NULL;
-- 点击数据
CREATE TABLE IF NOT EXISTS dw_zaful_recommend.zaful_click_sku(glb_oi STRING,sku STRING);
INSERT OVERWRITE TABLE dw_zaful_recommend.zaful_click_sku
SELECT
  *
FROM
  (
    SELECT
      glb_oi,
      GET_JSON_OBJECT(glb_skuinfo, '$.sku') sku
    FROM
      stg.zf_pc_event_info
    WHERE
      CONCAT(year, month, day) BETWEEN ${DATE}
      AND ${ADD_TIME_W}
      AND glb_t = 'ic'
      AND glb_skuinfo != ''
  ) a
WHERE
  glb_oi IS NOT NULL
  AND sku IS NOT NULL;
--   融合数据
CREATE TABLE IF NOT EXISTS dw_zaful_recommend.zaful_bury_sku(glb_oi STRING,sku STRING ,ways STRING);
INSERT OVERWRITE TABLE dw_zaful_recommend.zaful_bury_sku
SELECT
  glb_oi,
  sku,
  'ie' AS ways
FROM
  dw_zaful_recommend.zaful_pv_sku
UNION ALL
SELECT
  glb_oi,
  sku,
  'ic' AS ways
FROM
  dw_zaful_recommend.zaful_click_sku;
--过滤session会话
CREATE TABLE IF NOT EXISTS dw_zaful_recommend.zaful_bury_sku_filter(glb_oi STRING,sku STRING ,ways STRING);

INSERT OVERWRITE TABLE dw_zaful_recommend.zaful_bury_sku_filter
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
          dw_zaful_recommend.zaful_bury_sku
        group by
          glb_oi
      ) a
    WHERE
      cnt BETWEEN 20
      AND 1000
      AND glb_oi != ''
  ) b
  JOIN dw_zaful_recommend.zaful_bury_sku c on b.glb_oi = c.glb_oi;
--以session会话为单位聚合操作
CREATE TABLE IF NOT EXISTS dw_zaful_recommend.zaful_bury_sku_group(glb_oi STRING,ways STRING,skustr STRING);
INSERT OVERWRITE TABLE dw_zaful_recommend.zaful_bury_sku_group
select
  glb_oi,
  ways,
  concat_ws(',', collect_set(sku)) as skustr
from
  dw_zaful_recommend.zaful_bury_sku_filter
group by
  glb_oi,
  ways;
--商品的折扣
  INSERT OVERWRITE TABLE dw_zaful_recommend.goods_discount_tmp_1
SELECT
  goods_sn,
  AVG(goods_price) avg_price
FROM
  stg.zaful_eload_order_goods
WHERE
  addtime >= UNIX_TIMESTAMP(
    DATE_SUB(FROM_UNIXTIME(UNIX_TIMESTAMP(), 'yyyy-MM-dd'), 1),
    'yyyy-MM-dd'
  )
GROUP BY
  goods_sn;
INSERT OVERWRITE TABLE dw_zaful_recommend.goods_discount_tmp_3
SELECT
  goods_sn,
  AVG(goods_price) avg_price
FROM
  stg.zaful_eload_order_goods
WHERE
  addtime >= UNIX_TIMESTAMP(
    DATE_SUB(FROM_UNIXTIME(UNIX_TIMESTAMP(), 'yyyy-MM-dd'), 3),
    'yyyy-MM-dd'
  )
GROUP BY
  goods_sn;
INSERT OVERWRITE TABLE dw_zaful_recommend.goods_discount_tmp_7
SELECT
  goods_sn,
  AVG(goods_price) avg_price
FROM
  stg.zaful_eload_order_goods
WHERE
  addtime >= UNIX_TIMESTAMP(
    DATE_SUB(FROM_UNIXTIME(UNIX_TIMESTAMP(), 'yyyy-MM-dd'), 7),
    'yyyy-MM-dd'
  )
GROUP BY
  goods_sn;
INSERT OVERWRITE TABLE dw_zaful_recommend.goods_discount_tmp_14
SELECT
  goods_sn,
  AVG(goods_price) avg_price
FROM
  stg.zaful_eload_order_goods
WHERE
  addtime >= UNIX_TIMESTAMP(
    DATE_SUB(FROM_UNIXTIME(UNIX_TIMESTAMP(), 'yyyy-MM-dd'), 14),
    'yyyy-MM-dd'
  )
GROUP BY
  goods_sn;
INSERT OVERWRITE TABLE dw_zaful_recommend.goods_discount_table
SELECT
  goods_sn,
  IF(s1 IS NOT NULL, s1, 0) price_one,
  IF(s2 IS NOT NULL, s2, 0) price_three,
  IF(s3 IS NOT NULL, s3, 0) price_seven,
  IF(s4 IS NOT NULL, s4, 0) price_fourteen,
  shop_price
FROM
  (
    SELECT
      g.*,
      h.shop_price
    FROM
      (
        SELECT
          b.goods_sn,
          c.avg_price as s1,
          d.avg_price as s2,
          e.avg_price as s3,
          f.avg_price as s4
        FROM
          (
            SELECT
              goods_sn
            FROM
              (
                SELECT
                  goods_sn
                FROM
                  dw_zaful_recommend.goods_discount_tmp_1
                UNION ALL
                SELECT
                  goods_sn
                FROM
                  dw_zaful_recommend.goods_discount_tmp_3
                UNION ALL
                SELECT
                  goods_sn
                FROM
                  dw_zaful_recommend.goods_discount_tmp_7
                UNION ALL
                SELECT
                  goods_sn
                FROM
                  dw_zaful_recommend.goods_discount_tmp_14
              ) a
            GROUP BY
              goods_sn
          ) b
          LEFT JOIN dw_zaful_recommend.goods_discount_tmp_1 c ON b.goods_sn = c.goods_sn
          LEFT JOIN dw_zaful_recommend.goods_discount_tmp_3 d ON b.goods_sn = d.goods_sn
          LEFT JOIN dw_zaful_recommend.goods_discount_tmp_7 e ON b.goods_sn = e.goods_sn
          LEFT JOIN dw_zaful_recommend.goods_discount_tmp_14 f ON b.goods_sn = f.goods_sn
      ) g
      JOIN stg.zaful_eload_goods h ON g.goods_sn = h.goods_sn
  ) m;
