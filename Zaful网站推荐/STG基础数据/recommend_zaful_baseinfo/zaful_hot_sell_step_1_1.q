--Author cuijian
--zaful热销spu商品统计
--step 1 计算15天数据统计情况
set mapreduce.job.queuename=root.ai.offline;
USE stg;--一级分类-末级分类统计
CREATE TABLE IF NOT EXISTS dw_zaful_recommend.zaful_cate_level(
  cat_id INT,
  cat_name STRING,
  level INT,
  node1 STRING,
  node2 STRING,
  node3 STRING,
  node4 STRING
);
INSERT OVERWRITE TABLE dw_zaful_recommend.zaful_cate_level
SELECT
  cat_id,
  cat_name,
  level,
  nodes [0],
  CASE WHEN SIZE(nodes) > 1 THEN nodes [1] ELSE NULL END,
  CASE WHEN SIZE(nodes) > 2 THEN nodes [2] ELSE NULL END,
  CASE WHEN SIZE(nodes) > 3 THEN nodes [3] ELSE NULL END
FROM
  (
    SELECT
      cat_id,
      cat_name,
      level,
      SPLIT(node, ',') nodes
    FROM
      zaful_eload_category
  ) a;
-- 最近15天订单数据统计
  CREATE TABLE IF NOT EXISTS dw_zaful_recommend.goods_sku_order_count_15days (
    goods_sn STRING COMMENT '商品sku',
    category_id INT COMMENT '商品分类',
    qty INT COMMENT '订单商品数量'
  ) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' LINES TERMINATED BY '\n' STORED AS orc;
INSERT OVERWRITE TABLE dw_zaful_recommend.goods_sku_order_count_15days
SELECT
  b.goods_sn,
  d.cat_id,
  b.goods_number
FROM
  (
    SELECT
      a.goods_sn,
      a.goods_number
    FROM
      zaful_eload_order_goods a
      JOIN (
        SELECT
          order_id
        FROM
          zaful_eload_order_info
        WHERE
          order_status != 0
          AND order_status != 11
          AND add_time < UNIX_TIMESTAMP()
          AND add_time > UNIX_TIMESTAMP(
            DATE_SUB(
              FROM_UNIXTIME(UNIX_TIMESTAMP(), 'yyyy-MM-dd'),
              15
            ),
            'yyyy-MM-dd'
          )
      ) e ON a.order_id = e.order_id
  ) b
  JOIN (
    SELECT
      goods_sn,
      cat_id
    FROM
      zaful_eload_goods
  ) d ON b.goods_sn = d.goods_sn;
--最近15天热销前50数据
  CREATE TABLE IF NOT EXISTS dw_zaful_recommend.goods_sku_hotsell_15days (
    goods_sn STRING COMMENT '商品sku',
    category_id INT COMMENT '商品分类',
    sellcount INT COMMENT '商品销量',
    spurank INT COMMENT '商品排名'
  ) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' LINES TERMINATED BY '\n' STORED AS orc;
INSERT OVERWRITE TABLE dw_zaful_recommend.goods_sku_hotsell_15days
SELECT
  goods_sn,
  level,
  sellcount,
  spurank
FROM
  (
    SELECT
      m.goods_sn,
      m.level,
      sellcount,
      ROW_NUMBER() OVER(
        PARTITION BY 
        m.level
        ORDER BY
          sellcount DESC
      ) spurank
    FROM
      (
        SELECT
          a.goods_sn,
          b.node1 AS level,
          SUM(a.qty) sellcount
        FROM
          dw_zaful_recommend.goods_sku_order_count_15days a
          JOIN (
            SELECT
              node1,
              cat_id
            FROM
              dw_zaful_recommend.zaful_cate_level
            WHERE
              node1 IS NOT NULL
          ) b ON a.category_id = b.cat_id
        GROUP BY
          a.goods_sn,
          b.node1
        UNION ALL
        SELECT
          c.goods_sn,
          d.node2 AS level,
          SUM(c.qty) sellcount
        FROM
          dw_zaful_recommend.goods_sku_order_count_15days c
          JOIN (
            SELECT
              node2,
              cat_id
            FROM
              dw_zaful_recommend.zaful_cate_level
            WHERE
              node2 IS NOT NULL
          ) d ON c.category_id = d.cat_id
        GROUP BY
          c.goods_sn,
          d.node2
        UNION ALL
        SELECT
          e.goods_sn,
          f.node3 AS level,
          SUM(e.qty) sellcount
        FROM
          dw_zaful_recommend.goods_sku_order_count_15days e
          JOIN (
            SELECT
              node3,
              cat_id
            FROM
              dw_zaful_recommend.zaful_cate_level
            WHERE
              node3 IS NOT NULL
          ) f ON e.category_id = f.cat_id
        GROUP BY
          e.goods_sn,
          f.node3
        UNION ALL
        SELECT
          g.goods_sn,
          h.node4 AS level,
          SUM(g.qty) sellcount
        FROM
          dw_zaful_recommend.goods_sku_order_count_15days g
          JOIN (
            SELECT
              node4,
              cat_id
            FROM
              dw_zaful_recommend.zaful_cate_level
            WHERE
              node4 IS NOT NULL
          ) h ON g.category_id = h.cat_id
        GROUP BY
          g.goods_sn,
          h.node4
      ) m
  ) n
WHERE
  spurank < 51;