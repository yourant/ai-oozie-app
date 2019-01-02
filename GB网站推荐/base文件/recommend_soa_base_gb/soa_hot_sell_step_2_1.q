--Author cuijian
--soa热销spu商品统计
--15天销量补全代码
--将不足50个数据的分类向上补全至50个
--建立渠道与类别的关系表
set mapred.job.queue.name=root.ai.online; 
USE stg;
set mapred.max.split.size=100000000;
set mapred.min.split.size.per.node=100000000;
set mapred.min.split.size.per.rack=100000000;
set hive.auto.convert.join = false; 
set hive.exec.reducers.bytes.per.reducer=500000000;
set hive.groupby.skewindata=true;
set hive.input.format=org.apache.hadoop.hive.ql.io.BucketizedHiveInputFormat;
CREATE TABLE IF NOT EXISTS dw_gearbest_recommend.goods_channel_level(pipeline_code STRING COMMENT '渠道编码',category_id INT COMMENT '商品分类');
INSERT OVERWRITE TABLE dw_gearbest_recommend.goods_channel_level
SELECT
  pipeline_code,
  category_id
FROM
  dw_gearbest_recommend.goods_spu_hotsell_15days
GROUP BY
  pipeline_code,
  category_id;

--梳理出5级分类的数据
CREATE TABLE IF NOT EXISTS dw_gearbest_recommend.goods_hotsell_15days_autocomplete_mid(
  pipeline_code STRING COMMENT '渠道编码',
  category_id INT COMMENT '商品分类',
  goods_spu STRING COMMENT '商品spu',
  sellcount INT COMMENT '商品销量'
  );


INSERT OVERWRITE TABLE dw_gearbest_recommend.goods_hotsell_15days_autocomplete_mid 
SELECT
  b.pipeline_code,
  b.category_id,
  b.goods_spu,
  b.sellcount
FROM
  (
    SELECT
      id
    FROM
      dw_gearbest_recommend.goods_category_level
    WHERE
      level_cnt = 5
  ) a
  JOIN dw_gearbest_recommend.goods_spu_hotsell_15days b ON b.category_id = a.id;
--梳理四级分类，五级分类不够的需要四级分类去补充
INSERT INTO
  TABLE dw_gearbest_recommend.goods_hotsell_15days_autocomplete_mid
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
      ROW_NUMBER() OVER (
        PARTITION BY pipeline_code,
        category_id
        ORDER BY
          sellcount DESC
      ) spurank
    FROM
      (
        SELECT
          e.pipeline_code, 
          e.category_id,
          f.goods_spu,
          f.sellcount
        FROM
          (
            SELECT
              a.pipeline_code,
              a.category_id,
              b.level_4
            FROM
              (
                SELECT
                  pipeline_code,
                  category_id,
                  COUNT(DISTINCT goods_spu) cnt
                FROM
                  dw_gearbest_recommend.goods_hotsell_15days_autocomplete_mid
                GROUP BY
                  pipeline_code,
                  category_id
                HAVING
                  cnt < 50
              ) a
              JOIN dw_gearbest_recommend.goods_category_level b ON a.category_id = b.id
            UNION ALL
            SELECT
              c.pipeline_code,
              c.category_id,
              d.level_4
            FROM
              dw_gearbest_recommend.goods_channel_level c
              JOIN (
                SELECT
                  id,
                  level_4
                FROM
                  dw_gearbest_recommend.goods_category_level
                WHERE
                  level_cnt = 4
              ) d ON c.category_id = d.id
          ) e
          JOIN dw_gearbest_recommend.goods_spu_hotsell_15days f ON e.pipeline_code = f.pipeline_code
          AND e.level_4 = f.category_id

          GROUP BY e.pipeline_code,
          e.category_id,
          f.goods_spu,
          f.sellcount
      ) g
  ) h
WHERE
  spurank < 51;

--梳理三级分类
INSERT INTO
  TABLE dw_gearbest_recommend.goods_hotsell_15days_autocomplete_mid
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
      ROW_NUMBER() OVER (
        PARTITION BY pipeline_code,
        category_id
        ORDER BY
          sellcount DESC
      ) spurank
    FROM
      (
        SELECT
          e.pipeline_code,
          e.category_id,
          f.goods_spu,
          f.sellcount
        FROM
          (
            SELECT
              a.pipeline_code,
              a.category_id,
              b.level_3
            FROM
              (
                SELECT
                  pipeline_code,
                  category_id,
                  COUNT(DISTINCT goods_spu) cnt
                FROM
                  dw_gearbest_recommend.goods_hotsell_15days_autocomplete_mid
                GROUP BY
                  pipeline_code,
                  category_id
                HAVING
                  cnt < 50
              ) a
              JOIN dw_gearbest_recommend.goods_category_level b ON a.category_id = b.id
            UNION ALL
            SELECT
              c.pipeline_code,
              c.category_id,
              d.level_3
            FROM
              dw_gearbest_recommend.goods_channel_level c
              JOIN (
                SELECT
                  id,
                  level_3
                FROM
                  dw_gearbest_recommend.goods_category_level
                WHERE
                  level_cnt = 3
              ) d ON c.category_id = d.id
          ) e
          JOIN dw_gearbest_recommend.goods_spu_hotsell_15days f ON e.pipeline_code = f.pipeline_code
          AND e.level_3 = f.category_id
          GROUP BY e.pipeline_code,
          e.category_id,
          f.goods_spu,
          f.sellcount
      ) g
  ) h
WHERE
  spurank < 51;

--梳理二级分类
INSERT INTO
  TABLE dw_gearbest_recommend.goods_hotsell_15days_autocomplete_mid
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
      ROW_NUMBER() OVER (
        PARTITION BY pipeline_code,
        category_id
        ORDER BY
          sellcount DESC
      ) spurank
    FROM
      (
        SELECT
          e.pipeline_code,
          e.category_id,
          f.goods_spu,
          f.sellcount
        FROM
          (
            SELECT
              a.pipeline_code,
              a.category_id,
              b.level_2
            FROM
              (
                SELECT
                  pipeline_code,
                  category_id,
                  COUNT(DISTINCT goods_spu) cnt
                FROM
                  dw_gearbest_recommend.goods_hotsell_15days_autocomplete_mid
                GROUP BY
                  pipeline_code,
                  category_id
                HAVING
                  cnt < 50
              ) a
              JOIN dw_gearbest_recommend.goods_category_level b ON a.category_id = b.id
            UNION ALL
            SELECT
              c.pipeline_code,
              c.category_id,
              d.level_2
            FROM
              dw_gearbest_recommend.goods_channel_level c
              JOIN (
                SELECT
                  id,
                  level_2
                FROM
                  dw_gearbest_recommend.goods_category_level
                WHERE
                  level_cnt = 2
              ) d ON c.category_id = d.id
          ) e
          JOIN dw_gearbest_recommend.goods_spu_hotsell_15days f ON e.pipeline_code = f.pipeline_code
          AND e.level_2 = f.category_id
          GROUP BY e.pipeline_code,
          e.category_id,
          f.goods_spu,
          f.sellcount
      ) g
  ) h
WHERE
  spurank < 51;

--梳理一级分类

INSERT INTO
  TABLE dw_gearbest_recommend.goods_hotsell_15days_autocomplete_mid
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
      ROW_NUMBER() OVER (
        PARTITION BY pipeline_code,
        category_id
        ORDER BY
          sellcount DESC
      ) spurank
    FROM
      (
        SELECT
          e.pipeline_code,
          e.category_id,
          f.goods_spu,
          f.sellcount
        FROM
          (
            SELECT
              a.pipeline_code,
              a.category_id,
              b.level_1
            FROM
              (
                SELECT
                  pipeline_code,
                  category_id,
                  COUNT(DISTINCT goods_spu) cnt
                FROM
                  dw_gearbest_recommend.goods_hotsell_15days_autocomplete_mid
                GROUP BY
                  pipeline_code,
                  category_id
                HAVING
                  cnt < 50
              ) a
              JOIN dw_gearbest_recommend.goods_category_level b ON a.category_id = b.id
            UNION ALL
            SELECT
              c.pipeline_code,
              c.category_id,
              d.level_1
            FROM
              dw_gearbest_recommend.goods_channel_level c
              JOIN (
                SELECT
                  id,
                  level_1
                FROM
                  dw_gearbest_recommend.goods_category_level
                WHERE
                  level_cnt = 1
              ) d ON c.category_id = d.id
          ) e
          JOIN dw_gearbest_recommend.goods_spu_hotsell_15days f ON e.pipeline_code = f.pipeline_code
          AND e.level_1 = f.category_id
          GROUP BY e.pipeline_code,
          e.category_id,
          f.goods_spu,
          f.sellcount

      ) g
  ) h
WHERE
  spurank < 51;