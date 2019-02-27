--Author cuijian
--step 2
--zaful热销sku商品统计
--15天销量补全代码
--将不足50个数据的分类向上补全至50个
--过滤筛选下架和无库存商品，推荐结果每个分类输出12个
set mapreduce.job.queuename=root.ai.offline;
USE stg;
CREATE TABLE IF NOT EXISTS dw_zaful_recommend.goods_hotsell_15days_autocomplete_mid(
  category_id INT COMMENT '商品分类',
  goods_sn STRING COMMENT '商品sku',
  sellcount INT COMMENT '商品销量'
  );

--梳理出四级分类的数据
INSERT OVERWRITE TABLE dw_zaful_recommend.goods_hotsell_15days_autocomplete_mid 
SELECT
  b.category_id,
  b.goods_sn,
  b.sellcount
FROM
  (
    SELECT
      cat_id
    FROM
      dw_zaful_recommend.zaful_cate_level
    WHERE
      level = 4
  ) a
  JOIN dw_zaful_recommend.goods_sku_hotsell_15days b ON b.category_id = a.cat_id;
--梳理三级分类，四级分类不够的需要三级分类去补充
INSERT INTO
  TABLE dw_zaful_recommend.goods_hotsell_15days_autocomplete_mid
SELECT
  category_id,
  goods_sn,
  sellcount
FROM
  (
    SELECT

      category_id,
      goods_sn,
      sellcount,
      ROW_NUMBER() OVER (
        PARTITION BY
        category_id
        ORDER BY
          sellcount DESC
      ) spurank
    FROM
      (
        SELECT   
          e.category_id,
          f.goods_sn,
          f.sellcount
        FROM
          (
            SELECT
              
              a.category_id,
              b.node3
            FROM
              (
                SELECT
            
                  category_id  ,
                  COUNT(DISTINCT goods_sn) cnt
                FROM
                  dw_zaful_recommend.goods_hotsell_15days_autocomplete_mid
                GROUP BY
                  category_id
                HAVING
                  cnt < 50
              ) a
              JOIN dw_zaful_recommend.zaful_cate_level b ON a.category_id = b.cat_id
            UNION ALL
       
            SELECT
                cat_id AS category_id,
                node3
            FROM
                dw_zaful_recommend.zaful_cate_level
            WHERE
                level = 3
          
          ) e
          JOIN dw_zaful_recommend.goods_sku_hotsell_15days f ON 
         e.node3 = f.category_id

          GROUP BY 
          e.category_id,
          f.goods_sn,
          f.sellcount
      ) g
  ) h
WHERE
  spurank < 51;

--梳理二级级分类
INSERT INTO
  TABLE dw_zaful_recommend.goods_hotsell_15days_autocomplete_mid
SELECT
  category_id,
  goods_sn,
  sellcount
FROM
  (
    SELECT

      category_id,
      goods_sn,
      sellcount,
      ROW_NUMBER() OVER (
        PARTITION BY
        category_id
        ORDER BY
          sellcount DESC
      ) spurank
    FROM
      (
        SELECT   
          e.category_id,
          f.goods_sn,
          f.sellcount
        FROM
          (
            SELECT
              
              a.category_id,
              b.node2
            FROM
              (
                SELECT
            
                  category_id  ,
                  COUNT(DISTINCT goods_sn) cnt
                FROM
                  dw_zaful_recommend.goods_hotsell_15days_autocomplete_mid
                GROUP BY
                  category_id
                HAVING
                  cnt < 50
              ) a
              JOIN dw_zaful_recommend.zaful_cate_level b ON a.category_id = b.cat_id
            UNION ALL
       
            SELECT
                cat_id AS category_id,
                node2
            FROM
                dw_zaful_recommend.zaful_cate_level
            WHERE
                level = 2
          
          ) e
          JOIN dw_zaful_recommend.goods_sku_hotsell_15days f ON 
         e.node2 = f.category_id

          GROUP BY 
          e.category_id,
          f.goods_sn,
          f.sellcount
      ) g
  ) h
WHERE
  spurank < 51;

--梳理一级分类
INSERT INTO
  TABLE dw_zaful_recommend.goods_hotsell_15days_autocomplete_mid
SELECT
  category_id,
  goods_sn,
  sellcount
FROM
  (
    SELECT

      category_id,
      goods_sn,
      sellcount,
      ROW_NUMBER() OVER (
        PARTITION BY
        category_id
        ORDER BY
          sellcount DESC
      ) spurank
    FROM
      (
        SELECT   
          e.category_id,
          f.goods_sn,
          f.sellcount
        FROM
          (
            SELECT
              
              a.category_id,
              b.node1
            FROM
              (
                SELECT
            
                  category_id  ,
                  COUNT(DISTINCT goods_sn) cnt
                FROM
                  dw_zaful_recommend.goods_hotsell_15days_autocomplete_mid
                GROUP BY
                  category_id
                HAVING
                  cnt < 50
              ) a
              JOIN dw_zaful_recommend.zaful_cate_level b ON a.category_id = b.cat_id
            UNION ALL
       
            SELECT
                cat_id AS category_id,
                node1
            FROM
                dw_zaful_recommend.zaful_cate_level
            WHERE
                level = 1
          
          ) e
          JOIN dw_zaful_recommend.goods_sku_hotsell_15days f ON 
         e.node1 = f.category_id

          GROUP BY 
          e.category_id,
          f.goods_sn,
          f.sellcount
      ) g
  ) h
WHERE
  spurank < 51;

--输出分类推荐结果
CREATE TABLE IF NOT EXISTS dw_zaful_recommend.zaful_detailpage_app_rule_abtest(
  category_id INT COMMENT '商品分类',
  goods_sn STRING COMMENT '商品sku',
  sellcount INT COMMENT '商品销量'
  );
INSERT OVERWRITE TABLE dw_zaful_recommend.zaful_detailpage_app_rule_abtest
SELECT
  category_id,
  goods_sn,
  sellcount
FROM
  (
    SELECT
      category_id,
      goods_sn,
      sellcount,
      ROW_NUMBER() OVER(
        PARTITION BY category_id
        ORDER BY
          sellcount DESC
      ) rank_sec
    FROM
      (
        SELECT
          category_id,
          goods_sn,
          sellcount
        FROM
          (
            SELECT
              category_id,
              goods_sn,
              sellcount,
              spu,
              ROW_NUMBER() OVER(
                PARTITION BY category_id,
                spu
                ORDER BY
                  sellcount DESC
              ) rankid
            FROM
              (
                SELECT
                  category_id,
                  goods_sn,
                  sellcount,
                  SUBSTR(goods_sn, 1, 7) spu
                FROM
                  (
                    SELECT
                      b.*
                    FROM
                      (
                        SELECT
                          goods_sn
                        FROM
                          zaful_eload_goods
                        WHERE
                          is_on_sale = 1
                          AND goods_number > 0
                      ) a
                      JOIN dw_zaful_recommend.goods_hotsell_15days_autocomplete_mid b ON a.goods_sn = b.goods_sn
                  ) c
              ) d
          ) e
        WHERE
          rankid = 1
      ) f
  ) g
WHERE
  rank_sec < 31;

--获取上架商品的分类信息
CREATE TABLE IF NOT EXISTS dw_zaful_recommend.zaful_online_sku(
  category_id INT COMMENT '商品分类',
  goods_sn STRING COMMENT '商品sku'
);
INSERT
  OVERWRITE TABLE dw_zaful_recommend.zaful_online_sku
SELECT
  cat_id,
  goods_sn
FROM
  zaful_eload_goods
WHERE
  is_on_sale = 1;
--获取每一个商品的推荐结果
CREATE TABLE IF NOT EXISTS dw_zaful_recommend.zaful_detailpage_app_rule_final_abtest(
    goods_sn1 STRING COMMENT '商品sku 1',
    goods_sn2 STRING COMMENT '商品sku 2',
    sellcount INT COMMENT '商品销量'
  );
INSERT
  OVERWRITE TABLE dw_zaful_recommend.zaful_detailpage_app_rule_final_abtest
SELECT
  a.goods_sn AS s1,
  b.goods_sn AS s2,
  b.sellcount
FROM
  dw_zaful_recommend.zaful_online_sku a
  JOIN dw_zaful_recommend.zaful_detailpage_app_rule_abtest b ON a.category_id = b.category_id;
  
--组合推荐商品信息
INSERT OVERWRITE TABLE dw_zaful_recommend.zaful_detailpage_app_rule_final_abtest_rank SELECT
	goods_sn1,
	goods_sn2,
	ROW_NUMBER () OVER (
		PARTITION BY goods_sn1
		ORDER BY
			sellcount DESC
	) AS score
FROM
	dw_zaful_recommend.zaful_detailpage_app_rule_final_abtest;


INSERT OVERWRITE TABLE  dw_zaful_recommend.apl_result_detail_page_abtest2_fact
SELECT
	NVL(a.goods_sn1,'')              ,
	NVL(b.goodssn,'')              ,
	NVL(b.goodsid,0)              ,
	NVL(b.catid,0)                ,
	NVL(b.goodstitle,'')           ,
	NVL(b.goodscolor,'')           ,
	NVL(b.goodssize,'')           ,
	NVL(b.gridurl,'')              ,
	NVL(b.pipelinecode,'')        ,
	NVL(b.shopcode,'')             ,
	NVL(b.webgoodSn,'')            ,
	NVL(b.lang,'en')                 ,
	NVL(b.warecode,0)             ,
	NVL(b.reviewcount,0)          ,
	NVL(b.avgrate,0)              ,
	NVL(b.shopprice,0)            ,
	NVL(b.favoritecount,0)        ,
	NVL(b.goodsnum,0)             ,
	NVL(b.imgurl,'')               ,
	NVL(b.thumburl,'')             ,
	NVL(b.thumbextendUrl,'')       ,
	NVL(b.urltitle,'')            ,
	NVL(a.score,0)
FROM
	(
		SELECT
			goods_sn1,
			goods_sn2,
			score
		FROM
			dw_zaful_recommend.zaful_detailpage_app_rule_final_abtest_rank
		) a
    JOIN
    dw_zaful_recommend.apl_zaful_result_attr_fact b
    ON
		a.goods_sn2 = b.goodssn
    ; 
