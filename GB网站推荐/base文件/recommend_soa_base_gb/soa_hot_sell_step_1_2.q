--Author cuijian
--soa热销spu商品统计
--step 1_2 计算30天数据统计情况
--计算30天数据spu热销
set mapred.job.queue.name=root.ai.online; 
USE stg;
set mapred.max.split.size=100000000;
set mapred.min.split.size.per.node=100000000;
set mapred.min.split.size.per.rack=100000000;
set hive.auto.convert.join = false; 
set hive.exec.reducers.bytes.per.reducer=500000000;
set hive.groupby.skewindata=true;
set hive.input.format=org.apache.hadoop.hive.ql.io.BucketizedHiveInputFormat;
CREATE TABLE IF NOT EXISTS dw_gearbest_recommend.goods_spu_order_count_30days(goods_spu STRING COMMENT '商品spu', category_id INT COMMENT '商品分类', qty INT COMMENT '订单商品数量',pipeline_code STRING COMMENT '渠道编码') 
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' LINES TERMINATED BY '\n' STORED AS orc;
INSERT OVERWRITE TABLE dw_gearbest_recommend.goods_spu_order_count_30days
SELECT
  c.goods_spu,
  d.category_id,
  b.qty,
  b.pipeline_code
FROM
  (
    SELECT
      a.goods_sn,
      a.qty,
      e.pipeline_code
    FROM
      gb_order_order_goods a

      JOIN
        (SELECT
          order_sn,pipeline_code
        FROM
          gb_order_order_info
        WHERE
          order_status=0
          AND created_time < UNIX_TIMESTAMP()
          AND created_time > UNIX_TIMESTAMP(
            DATE_SUB(FROM_UNIXTIME(UNIX_TIMESTAMP(), 'yyyy-MM-dd'), 30),'yyyy-MM-dd'
          )
      )e
      ON a.order_sn=e.order_sn
  ) b
  JOIN dw_gearbest_recommend.goods_info_mid1 c ON b.goods_sn = c.good_sn
  JOIN (
    SELECT
      good_sn,
      category_id
    FROM
      gb_goods_goods_category_relation
    WHERE
      is_default = 1
  ) d ON b.goods_sn = d.good_sn;
CREATE TABLE IF NOT EXISTS dw_gearbest_recommend.goods_spu_hotsell_30days
(goods_spu STRING COMMENT '商品spu', pipeline_code STRING COMMENT '渠道编码',category_id INT COMMENT '商品分类',sellcount INT COMMENT '商品销量',spurank INT COMMENT '商品排名') 
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' LINES TERMINATED BY '\n' STORED AS orc;
INSERT OVERWRITE TABLE dw_gearbest_recommend.goods_spu_hotsell_30days
SELECT
  goods_spu,
  pipeline_code,
  level,
  sellcount,
  spurank
FROM
  (
    SELECT
      m.pipeline_code,
      m.goods_spu,
      m.level,
      sellcount,
      ROW_NUMBER() OVER(
        PARTITION BY m.pipeline_code,m.level
        ORDER BY
          sellcount DESC
      ) spurank
    FROM
      (
        SELECT
          a.pipeline_code,
          a.goods_spu,
          b.level_1 AS level,
          SUM(a.qty) sellcount
        FROM
          dw_gearbest_recommend.goods_spu_order_count_30days a
          JOIN (
            SELECT
              level_1,id
            FROM
              dw_gearbest_recommend.goods_category_level
            WHERE
              level_1 IS NOT NULL
          ) b ON a.category_id = b.id
        GROUP BY
        a.pipeline_code,
          a.goods_spu,
          b.level_1
        UNION ALL
        SELECT
        c.pipeline_code,
          c.goods_spu,
          d.level_2 AS level,
          SUM(c.qty) sellcount
        FROM
          dw_gearbest_recommend.goods_spu_order_count_30days c
          JOIN (
            SELECT
              level_2,id
            FROM
              dw_gearbest_recommend.goods_category_level
            WHERE
              level_2 IS NOT NULL
          ) d ON c.category_id = d.id
        GROUP BY
        c.pipeline_code,
          c.goods_spu,
          d.level_2
        UNION ALL
        SELECT
        e.pipeline_code,
          e.goods_spu,
          f.level_3 AS level,
          SUM(e.qty) sellcount
        FROM
          dw_gearbest_recommend.goods_spu_order_count_30days e
          JOIN (
            SELECT
              level_3,id
            FROM
              dw_gearbest_recommend.goods_category_level
            WHERE
              level_3 IS NOT NULL
          ) f ON e.category_id = f.id
        GROUP BY
        e.pipeline_code,
          e.goods_spu,
          f.level_3
        UNION ALL
        SELECT
        g.pipeline_code,
          g.goods_spu,
          h.level_4 AS level,
          SUM(g.qty) sellcount
        FROM
          dw_gearbest_recommend.goods_spu_order_count_30days g
          JOIN (
            SELECT
              level_4,id
            FROM
              dw_gearbest_recommend.goods_category_level
            WHERE
              level_4 IS NOT NULL
          ) h ON g.category_id = h.id
        GROUP BY
        g.pipeline_code,
          g.goods_spu,
          h.level_4
        UNION ALL
        SELECT
        i.pipeline_code,
          i.goods_spu,
          j.level_5 AS level,
          SUM(i.qty) sellcount
        FROM
          dw_gearbest_recommend.goods_spu_order_count_30days i
          JOIN (
            SELECT
              level_5,id
            FROM
              dw_gearbest_recommend.goods_category_level
            WHERE
              level_5 IS NOT NULL
          ) j ON i.category_id = j.id
        GROUP BY
        i.pipeline_code,
          i.goods_spu,
          j.level_5
      ) m
  ) n
WHERE
  spurank < 51;