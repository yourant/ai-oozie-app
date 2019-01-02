--Author cuijian
--soa热销spu商品统计
--处理全平台热销和收藏夹排序分类
set mapred.job.queue.name=root.ai.online; 
USE stg;
set mapred.max.split.size=100000000;
set mapred.min.split.size.per.node=100000000;
set mapred.min.split.size.per.rack=100000000;
set hive.auto.convert.join = false; 
set hive.exec.reducers.bytes.per.reducer=500000000;
set hive.groupby.skewindata=true;
set hive.input.format=org.apache.hadoop.hive.ql.io.BucketizedHiveInputFormat;
--12802和11852分类单独计算用于用户中心页推荐
CREATE TABLE IF NOT EXISTS dw_gearbest_recommend.goods_hotsell_30days_supply_mid(
  pipeline_code STRING COMMENT '渠道编码',
  category_id INT COMMENT '商品分类',
  goods_spu STRING COMMENT '商品spu',
  sellcount INT COMMENT '商品销量'
  );
  CREATE TABLE IF NOT EXISTS dw_gearbest_recommend.goods_hotsell_30days_supply(
  pipeline_code STRING COMMENT '渠道编码',
  category_id INT COMMENT '商品分类',
  goods_spu STRING COMMENT '商品spu',
  sellcount INT COMMENT '商品销量'
  );
--查找4级分类
INSERT OVERWRITE TABLE dw_gearbest_recommend.goods_hotsell_30days_supply_mid
SELECT
  c.pipeline_code,
  b.level_3,
  c.goods_spu,
  c.sellcount
FROM
  (
    SELECT
      level_3,
      level_4
    FROM
      dw_gearbest_recommend.goods_category_level a
    WHERE
      a.level_3 in (12082, 11852)
      and level_4 IS NOT NULL
  ) b
  JOIN dw_gearbest_recommend.goods_spu_hotsell_30days c ON c.category_id = b.level_4
GROUP BY
  pipeline_code,
  level_3,
  goods_spu,
  sellcount;INSERT OVERWRITE TABLE dw_gearbest_recommend.goods_hotsell_30days_supply_mid
SELECT
  c.pipeline_code,
  b.level_3,
  c.goods_spu,
  c.sellcount
FROM
  (
    SELECT
      level_3,
      level_2
    FROM
      dw_gearbest_recommend.goods_category_level a
    WHERE
      a.level_3 in (12082, 11852)
      and level_2 IS NOT NULL
  ) b
  JOIN dw_gearbest_recommend.goods_spu_hotsell_30days c ON c.category_id = b.level_2
GROUP BY
  pipeline_code,
  level_3,
  goods_spu,
  sellcount;
INSERT OVERWRITE TABLE dw_gearbest_recommend.goods_hotsell_30days_supply
SELECT
  pipeline_code,
  category_id,
  goods_spu,
  sellcount
FROM
  (
    SELECT
      pipeline_code,
      category_id,
      goods_spu,
      sellcount,
      ROW_NUMBER() OVER(
        PARTITION BY pipeline_code,
        category_id
        ORDER BY
          sellcount DESC
      ) spurank
    FROM
      dw_gearbest_recommend.goods_hotsell_30days_supply_mid
  ) a
WHERE
  spurank < 51;

--全平台热销分类排序，不局限于分类信息
CREATE TABLE IF NOT EXISTS dw_gearbest_recommend.gb_paltform_hotsell_top100(goods_spu STRING COMMENT '商品spu',sellcount INT COMMENT '商品销量',spurank INT COMMENT '商品排名',pipeline_code STRING COMMENT '渠道编码') 
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' LINES TERMINATED BY '\n' STORED AS orc;
INSERT OVERWRITE TABLE dw_gearbest_recommend.gb_paltform_hotsell_top100
SELECT
  goods_spu,
  sellcount,
  spurank,
  pipeline_code
FROM
  (
    SELECT
      goods_spu,
      pipeline_code,
      sellcount,
      ROW_NUMBER() OVER(
        PARTITION BY pipeline_code
        ORDER BY
          sellcount DESC
      ) spurank
    FROM
      (
        SELECT
          d.goods_spu,
          c.pipeline_code,
          SUM(c.qty) sellcount
        FROM
          (
            SELECT
              a.goods_sn,
              a.qty,
              b.pipeline_code
            FROM
              gb_order_order_goods a
              JOIN (
                SELECT
                  order_sn,
                  pipeline_code
                FROM
                  gb_order_order_info
                WHERE
                  order_status = 0
                  AND created_time < UNIX_TIMESTAMP()
                  AND created_time > UNIX_TIMESTAMP(
                    DATE_SUB(
                      FROM_UNIXTIME(UNIX_TIMESTAMP(), 'yyyy-MM-dd'),
                      30
                    ),
                    'yyyy-MM-dd'
                  )
              ) b ON a.order_sn = b.order_sn
          ) c
          JOIN dw_gearbest_recommend.goods_info_mid1 d ON c.goods_sn = d.good_sn
        GROUP BY
          d.goods_spu,
          c.pipeline_code
      ) f
  ) g
WHERE
  spurank < 101;
--全平台收藏排序，选取top10
CREATE TABLE IF NOT EXISTS dw_gearbest_recommend.gb_platform_favorite_top10(goods_spu STRING COMMENT '商品spu',favorite_count INT COMMENT '商品销量',spurank INT COMMENT '商品排名',pipeline_code STRING COMMENT '渠道编码') 
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' LINES TERMINATED BY '\n' STORED AS orc;
INSERT OVERWRITE TABLE dw_gearbest_recommend.gb_platform_favorite_top10
SELECT
  goods_spu,
  favorite_count,
  spurank,
  pipeline_code
FROM
  (
    SELECT
      goods_spu,
      pipeline_code,
      favorite_count,
      ROW_NUMBER() OVER(
        PARTITION BY pipeline_code
        ORDER BY
          favorite_count DESC
      ) spurank
    FROM
      (
        SELECT
          d.goods_spu,
          c.pipeline_code,
          COUNT(1) favorite_count
        FROM
          (
            SELECT
              good_sn,
              pipeline_code
            FROM
              gb_member_mem_favorites
            WHERE
              create_date < UNIX_TIMESTAMP()
              AND create_date > UNIX_TIMESTAMP(
                DATE_SUB(
                  FROM_UNIXTIME(UNIX_TIMESTAMP(), 'yyyy-MM-dd'),
                  30
                ),
                'yyyy-MM-dd'
              )
          ) c
          JOIN dw_gearbest_recommend.goods_info_mid1 d ON c.good_sn = d.good_sn
        GROUP BY
          d.goods_spu,
          c.pipeline_code
      ) f
  ) g
WHERE
  spurank < 11;