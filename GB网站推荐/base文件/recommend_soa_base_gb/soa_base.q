--@author LiuQingFan
--@date 2017年11月25日 下午 16:00
--@desc  SOA_gb基础数据

set mapred.job.queue.name=root.ai.offline; 
set mapred.job.name='apl_soa_base_fact';
SET mapred.max.split.size=128000000;
SET mapred.min.split.size=32000000;
SET mapred.min.split.size.per.node=32000000;
SET mapred.min.split.size.per.rack=32000000;
SET hive.exec.reducers.bytes.per.reducer = 128000000;
SET hive.merge.mapfiles=true;
SET hive.merge.mapredfiles= true;
SET hive.merge.size.per.task=256000000;
SET hive.exec.parallel = true; 
set hive.support.concurrency=false;
set hive.auto.convert.join=false;
set hive.vectorized.execution.enabled=false; 
USE  dw_gearbest_recommend;

CREATE TABLE IF NOT EXISTS goods_info_mid1(
	good_sn            string     COMMENT '商品SKU',
	goods_spu          string     COMMENT '商品SPU',
	goods_web_sku      string     COMMENT '商品webSku',
	shop_code          bigint     COMMENT '商品店铺CODE',
	goods_status       int        COMMENT '商品状态',
	brand_code         string     COMMENT '品牌CODE',
	first_up_time      bigint     COMMENT '商品首次上架时间',
	v_wh_code          int        COMMENT '商品虚拟销售', 
	shop_price         double     COMMENT '本店售价',
	id                 int        COMMENT '分类ID',
	level_cnt          int        COMMENT 'SKU所属分类等级',
	level_1            int        COMMENT '商品对应一级分类',
	level_2            int        COMMENT '商品对应二级分类',
	level_3            int        COMMENT '商品对应三级分类',
	level_4            int        COMMENT '商品对应四级分类',
	pipeline_code      string     COMMENT '商品销售渠道编码',
	url_title          string     COMMENT '静态页面文件标题'
	)
COMMENT '商品信息中间表1（分类、上下架、仓库、状态等信息）'
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\u0001'                                                                                   
LINES TERMINATED BY '\n'                                                                                          
STORED AS TEXTFILE;

INSERT OVERWRITE TABLE goods_info_mid1
SELECT
	good_sn,
	goods_spu,
	goods_web_sku,
	shop_code,
	goods_status,
	brand_code,
	first_up_time,
	v_wh_code,
	shop_price,
	id, 
	level_cnt,
	level_1,
	level_2,
	level_3,
	level_4,
	pipeline_code,
	url_title
FROM(
	SELECT
		t1.good_sn,
		t1.goods_spu,
		t1.goods_web_sku,
		t1.shop_code,
		t2.goods_status,
		t1.brand_code,
		t2.first_up_time,
		--t2.v_wh_code,
		t_ods_pw.v_wh_code,
		t3.shop_price,
		t4.id, 
		t4.level_cnt,
		t4.level_1,
		t4.level_2,
		t4.level_3,
		t4.level_4,
		t6.pipeline_code,
		t1.url_title
	FROM
		stg_gb_goods.goods_info t1
	LEFT JOIN
		stg_gb_goods.goods_info_extend t2
	ON
		t1.good_sn = t2.good_sn
	LEFT JOIN
	  (select good_sn,pipeline_code,v_wh_code,shop_price from ods.ods_m_gearbest_gb_goods_goods_price_relation
	   where dt='${DATE}') t3
	ON
		t2.good_sn = t3.good_sn AND t2.v_wh_code = t3.v_wh_code
	LEFT JOIN
		(select pipeline_code,v_wh_code from ods.ods_m_gearbest_obs_gb_pipeline_warehouse
		where dt='${DATE}') t_ods_pw 
	ON	
		t_ods_pw.pipeline_code = t3.pipeline_code AND t_ods_pw.v_wh_code = t2.v_wh_code
	LEFT JOIN
		stg.gb_goods_goods_category_relation t5
	ON
		t1.good_sn = t5.good_sn
	LEFT JOIN
		--goods_category_level t4
		(select id, level_cnt,level_1,level_2,level_3,level_4 from ods.ods_o_gearbest_bigdata_goods_category_level where day='${DATE}') t4
	ON
		t5.category_id = t4.id
       JOIN
	   (select good_sn,pipeline_code from ods.ods_m_gearbest_gb_goods_goods_pipeline_relation
	   where dt='${DATE}') t6
	ON
		t1.good_sn = t6.good_sn AND t3.pipeline_code = t6.pipeline_code
	WHERE
		t5.is_default = 1 AND t2.goods_status = 2
	) tmp
WHERE
	pipeline_code != '' and v_wh_code != ''
GROUP BY
	good_sn,
	goods_spu,
	goods_web_sku,
	shop_code,
	goods_status,
	brand_code,
	first_up_time,
	v_wh_code,
	shop_price,
	id, 
	level_cnt,
	level_1,
	level_2,
	level_3,
	level_4,
	pipeline_code,
	url_title
;
	
CREATE TABLE IF NOT EXISTS goods_info_mid2(
	good_sn            string     COMMENT '商品SKU',
	good_title         string     COMMENT '商品title',
	img_url            string     COMMENT '产品图url',
	grid_url           string     COMMENT 'grid图url',
	thumb_url          string     COMMENT '缩略图url',
	thumb_extend_url   string     COMMENT '商品图片',
	original_url       string     COMMENT '商品原图',
	lang               string     COMMENT '语言'
	)
COMMENT '商品信息中间表2（网站展示用信息（图片、title））'
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\u0001'                                                                                   
LINES TERMINATED BY '\n'                                                                                          
STORED AS TEXTFILE;
	
INSERT OVERWRITE TABLE goods_info_mid2
SELECT
	t1.good_sn,
	regexp_replace(t3.good_title,'"','\\\\"'),
	t2.img_url,
	t2.grid_url,
	t2.thumb_url,
	t2.thumb_extend_url,
	t2.original_url,
	t3.lang
FROM
	stg.gb_goods_goods_picture t2
LEFT JOIN	
	(SELECT
		good_sn,
		type,
		version
	FROM
		stg.gb_goods_goods_gallery
	WHERE
		is_soa_use = 2 AND is_select = 1
	) t1
ON
	t2.type = t1.type AND t2.version = t1.version AND t1.good_sn = t2.good_sn
LEFT JOIN
	stg.gb_goods_site_multi_lang_goods t3
ON
	t2.good_sn = t3.good_sn
WHERE
	t2.is_main = 1 and (t2.img_url != '' and t2.grid_url != '' and t2.thumb_url != '' and t2.thumb_extend_url != '' and t2.original_url != '');

	
	
	
	
	
CREATE TABLE IF NOT EXISTS goods_info_mid3(
	good_sn            string     COMMENT '商品SKU',
	v_wh_code          int        COMMENT '商品虚拟销售仓',
	stock_qty          bigint     COMMENT '商品库存'
	)
COMMENT '商品信息中间表3（库存）'
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\u0001'                                                                                   
LINES TERMINATED BY '\n'                                                                                          
STORED AS TEXTFILE;


INSERT OVERWRITE TABLE goods_info_mid3
SELECT
	t.good_sn,
	t.v_wh_code,
	SUM(t.stock_qty)
FROM(
	SELECT
		t1.good_sn,
		t1.v_wh_code,
		t2.r_wh_code,
		t2.is_public,
		t3.public_stock  stock_qty  
	FROM
		stg.gb_goods_goods_info t1
	JOIN
		stg_stock.virtual_really_warehouse_relation t2
	ON
		t1.v_wh_code = t2.v_wh_code
	JOIN 
		stg_stock.goods_stock_warehouse_relation t3
	ON
		t1.good_sn = t3.good_sn AND t2.r_wh_code = t3.r_wh_code
	WHERE
		t2.is_public = 1
	UNION ALL
	SELECT
		t1.good_sn,
		t1.v_wh_code,
		t2.r_wh_code,
		t2.is_public,
		t3.private_stock stock_qty
	FROM
		stg.gb_goods_goods_info t1
	JOIN
		stg_stock.virtual_really_warehouse_relation t2
	ON
		t1.v_wh_code = t2.v_wh_code
	JOIN
		stg_stock.goods_stock_warehouse_relation t3
	ON
		t1.good_sn = t3.good_sn AND t2.r_wh_code = t3.r_wh_code
	WHERE
		t2.is_public = 2
	) t
GROUP BY
	t.good_sn,
	t.v_wh_code;
	
	

	




CREATE TABLE IF NOT EXISTS goods_info_mid4(
	good_sn            string        COMMENT '商品SKU',
	avg_score          decimal(2,1)  COMMENT '商品评分数',
	total_num          bigint        COMMENT '商品评论数量',
	total_favorite     bigint        COMMENT '商品收藏数量'
	)
COMMENT '商品信息中间表3（评论）'
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\u0001'                                                                                   
LINES TERMINATED BY '\n'                                                                                          
STORED AS TEXTFILE;




INSERT OVERWRITE TABLE goods_info_mid4
SELECT
	t1.good_sn,
	t3.avg_score,
	t2.total_num,
	t4.num
FROM
	stg.gb_goods_goods_info t1
LEFT JOIN
	(SELECT
			goods_sku,
			count(1) total_num
		FROM
			ods.ods_m_gearbest_review_service_gearbest_review
		GROUP BY
			goods_sku
		) t2
ON
	t1.good_sn = t2.goods_sku
LEFT JOIN
	(
	SELECT
		goods_sku,
		avg(rate_overall) avg_score
	FROM(
		SELECT
			a.goods_sku,
			b.rate_overall
		FROM
			ods.ods_m_gearbest_review_service_gearbest_review a
		JOIN
			ods.ods_m_gearbest_review_service_gearbest_review_info b
		ON
			a.id = b.review_id
		) tmp
	GROUP BY
		goods_sku
	) t3
ON
	t1.good_sn = t3.goods_sku
LEFT JOIN
	(SELECT
		good_sn,
		count(*) num
	FROM
		stg_gb_member.mem_favorites
	GROUP BY
		good_sn
		) t4
ON
	t4.good_sn = t1.good_sn;

INSERT OVERWRITE TABLE goods_info_mid4
SELECT
	good_sn,
	avg_score,
	total_num,
	total_favorite
FROM
	goods_info_mid4
GROUP BY
	good_sn,
	avg_score,
	total_num,
	total_favorite;
	
	
	
CREATE TABLE IF NOT EXISTS goods_info_mid5(
	goods_spu            string        COMMENT '商品SPU',
	good_sn              string        COMMENT '商品SKU',
	category_id          int           COMMENT '对应分类',
	node1                int           COMMENT '一级分类',
	node2                int           COMMENT '二级分类',
	node3                int           COMMENT '三级分类',
	node4                int           COMMENT '四级分类'               
	)
COMMENT 'SPU/SKU一一对应表'
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\u0001'                                                                                   
LINES TERMINATED BY '\n'                                                                                          
STORED AS TEXTFILE;


INSERT OVERWRITE TABLE goods_info_mid5
SELECT
	goods_spu,
	good_sn,
	id,
	level_1,
	level_2,
	level_3,
	level_4
	FROM(
		SELECT
			good_sn,
			goods_spu,
			id,
			level_1,
			level_2,
			level_3,
			level_4,
			ROW_NUMBER() OVER(PARTITION BY goods_spu ORDER BY good_sn asc) AS flag
		FROM
			goods_info_mid1 
	) tmp
WHERE
	flag = 1;
	
	

	
	
CREATE TABLE IF NOT EXISTS goods_info_result(
	good_sn            string        COMMENT '商品SKU',
	goods_spu          string        COMMENT '商品SPU',
	goods_web_sku      string        COMMENT '商品webSku',
	shop_code          bigint        COMMENT '商品店铺CODE',
	goods_status       int           COMMENT '商品状态',
	brand_code         string        COMMENT '品牌CODE',
	first_up_time      bigint        COMMENT '商品首次上架时间',
	v_wh_code          int           COMMENT '商品虚拟销售', 
	shop_price         double        COMMENT '本店售价',
	id                 int           COMMENT '分类ID',
	level_cnt          int           COMMENT 'SKU所属分类等级',
	level_1            int           COMMENT '商品对应一级分类',
	level_2            int           COMMENT '商品对应二级分类',
	level_3            int           COMMENT '商品对应三级分类',
	level_4            int           COMMENT '商品对应四级分类',
	good_title         string        COMMENT '商品title',
	img_url            string        COMMENT '产品图url',
	grid_url           string        COMMENT 'grid图url',
	thumb_url          string        COMMENT '缩略图url',
	thumb_extend_url   string        COMMENT '商品图片',
	lang               string        COMMENT '语言',
	stock_qty          bigint        COMMENT '商品库存',
	avg_score          decimal(2,1)  COMMENT '商品评分数',
	total_num          bigint        COMMENT '商品评论数量',
	total_favorite     bigint        COMMENT '商品收藏数量',
	pipeline_code      string        COMMENT '商品销售渠道编码',
	url_title          string        COMMENT '静态页面文件标题'
	)
COMMENT '商品属性信息表'
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\u0001'                                                                                   
LINES TERMINATED BY '\n'                                                                                          
STORED AS TEXTFILE;

INSERT OVERWRITE TABLE 	goods_info_result
SELECT
	t1.good_sn,
	t1.goods_spu,
	t1.goods_web_sku,
	t1.shop_code,
	t1.goods_status,
	t1.brand_code,
	t1.first_up_time,
	t1.v_wh_code,
	t1.shop_price,
	t1.id, 
	t1.level_cnt,
	t1.level_1,
	t1.level_2,
	t1.level_3,
	t1.level_4,
	t2.good_title,
	t2.img_url,
	t2.grid_url,
	t2.thumb_url,
	t2.thumb_extend_url,
	t2.lang,
	t3.stock_qty,
	t4.avg_score,
	t4.total_num,
	t4.total_favorite,
	t1.pipeline_code,
	t1.url_title
FROM
	goods_info_mid1 t1
LEFT JOIN
	goods_info_mid2 t2
ON
	t1.good_sn = t2.good_sn
LEFT JOIN
	goods_info_mid3 t3
ON
	t1.good_sn = t3.good_sn AND t1.v_wh_code = t3.v_wh_code
LEFT JOIN
	goods_info_mid4 t4
ON
	t1.good_sn = t4.good_sn
-- LEFT JOIN
-- 	stg.gb_goods_goods_pipeline_info t5
-- ON
-- 	t1.good_sn = t5.good_sn AND t1.v_wh_code = t5.v_wh_code AND t5.pipeline_code = t1.pipeline_code
-- JOIN
--      stg.gb_goods_goods_pipeline_relation t6
-- ON
--     t1.good_sn = t6.good_sn AND t6.pipeline_code = t1.pipeline_code
;
CREATE TABLE IF NOT EXISTS sku_info_data(
	sku                string        COMMENT '商品SKU',
	id                 int           COMMENT '分类ID',
	cat1id             int           COMMENT '商品对应一级分类',
	cat2id             int           COMMENT '商品对应二级分类',
	cat3id             int           COMMENT '商品对应三级分类',
	cat4id             int           COMMENT '商品对应四级分类',
	addTime            bigint        COMMENT '商品首次上架时间',
	starlevel          decimal(2,1)  COMMENT '商品评分数',
	reviewNum          bigint        COMMENT '商品评论数量'
	)
COMMENT '商品属性信息表'
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\u0001'                                                                                   
LINES TERMINATED BY '\n'                                                                                          
STORED AS TEXTFILE;
	
INSERT OVERWRITE TABLE sku_info_data
SELECT
	DISTINCT
	good_sn,
	id,
	level_1,
	level_2,
	level_3,
	level_4,
	first_up_time,
	avg_score,
	total_num
FROM
	goods_info_result a;
	
CREATE TABLE IF NOT EXISTS goods_info_result_rec(
	good_sn            string        COMMENT '商品SKU',
	goods_spu          string        COMMENT '商品SPU',
	goods_web_sku      string        COMMENT '商品webSku',
	shop_code          bigint        COMMENT '商品店铺CODE',
	goods_status       int           COMMENT '商品状态',
	brand_code         string        COMMENT '品牌CODE',
	first_up_time      bigint        COMMENT '商品首次上架时间',
	v_wh_code          int           COMMENT '商品虚拟销售', 
	shop_price         double        COMMENT '本店售价',
	id                 int           COMMENT '分类ID',
	level_cnt          int           COMMENT 'SKU所属分类等级',
	level_1            int           COMMENT '商品对应一级分类',
	level_2            int           COMMENT '商品对应二级分类',
	level_3            int           COMMENT '商品对应三级分类',
	level_4            int           COMMENT '商品对应四级分类',
	good_title         string        COMMENT '商品title',
	img_url            string        COMMENT '产品图url',
	grid_url           string        COMMENT 'grid图url',
	thumb_url          string        COMMENT '缩略图url',
	thumb_extend_url   string        COMMENT '商品图片',
	lang               string        COMMENT '语言',
	stock_qty          bigint        COMMENT '商品库存',
	avg_score          decimal(2,1)  COMMENT '商品评分数',
	total_num          bigint        COMMENT '商品评论数量',
	total_favorite     bigint        COMMENT '商品收藏数量',
	pipeline_code      string        COMMENT '商品销售渠道编码',
	url_title          string        COMMENT '静态页面文件标题'
	)
COMMENT '可被推荐商品属性信息表'
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\u0001'                                                                                   
LINES TERMINATED BY '\n'                                                                                          
STORED AS TEXTFILE;

INSERT OVERWRITE TABLE goods_info_result_rec
SELECT
	good_sn            ,
	goods_spu          ,
	goods_web_sku      ,
	shop_code          ,
	goods_status       ,
	brand_code         ,
	first_up_time      ,
	v_wh_code          ,
	shop_price         ,
	id                 ,
	level_cnt          ,
	level_1            ,
	level_2            ,
	level_3            ,
	level_4            ,
	good_title         ,
	img_url            ,
	grid_url           ,
	thumb_url          ,
	thumb_extend_url   ,
	lang               ,
	stock_qty          ,
	avg_score          ,
	total_num          ,
	total_favorite     ,
	pipeline_code      ,
	url_title    
FROM
	goods_info_result
--业务方要求过去掉T+1过滤 ---20190508--xiongjun----
--WHERE
	--stock_qty > 0 AND goods_status = 2;


INSERT OVERWRITE TABLE dw_gearbest_recommend.goods_info_result_uniq 
SELECT
	m.good_sn,
	m.goods_spu,
	m.goods_web_sku,
	m.shop_code,
	m.goods_status,
	m.brand_code,
	m.first_up_time,
	m.v_wh_code,
	m.shop_price,
	m.id,
	m.level_cnt,
	m.level_1,
	m.level_2,
	m.level_3,
	m.level_4,
	m.good_title,
	m.img_url,
	m.grid_url,
	m.thumb_url,
	m.thumb_extend_url,
	m.lang,
	m.stock_qty,
	m.avg_score,
	m.total_num,
	m.total_favorite,
	m.pipeline_code,
	m.url_title
FROM
	(
		SELECT
			good_sn,
			goods_spu,
			goods_web_sku,
			shop_code,
			goods_status,
			brand_code,
			first_up_time,
			v_wh_code,
			shop_price,
			id,
			level_cnt,
			level_1,
			level_2,
			level_3,
			level_4,
			good_title,
			img_url,
			grid_url,
			thumb_url,
			thumb_extend_url,
			lang,
			stock_qty,
			avg_score,
			total_num,
			total_favorite,
			pipeline_code,
			url_title,
			ROW_NUMBER () OVER (
				PARTITION BY good_sn,
				pipeline_code,
				good_title,
				lang
			ORDER BY
				shop_price ASC
			) AS flag
            FROM dw_gearbest_recommend.goods_info_result_rec
	) m
WHERE
	m.flag = 1;

 --pipeline_language_map更新
INSERT OVERWRITE TABLE dw_gearbest_recommend.pipeline_language_map  SELECT
	pipeline_code,
	CASE
WHEN lang = 'en-gb' THEN
	'en'
WHEN lang = 'en-us' THEN
	'en'
WHEN lang = 'en-cbd' THEN
	'en'
ELSE
	lang
END AS lang
FROM
	dw_gearbest_recommend.pipeline_language
WHERE
	STATUS = '1'
GROUP BY
	pipeline_code,
	lang
	;

 --商品池表更新，取pipeline_code对应的lang数据，其他多语言丢弃
 INSERT overwrite TABLE dw_gearbest_recommend.goods_info_result_uniqlang SELECT
	n.good_sn,
	n.goods_spu,
	n.goods_web_sku,
	n.shop_code,
	n.goods_status,
	n.brand_code,
	n.first_up_time,
	n.v_wh_code,
	n.shop_price,
	n.id,
	n.level_cnt,
	n.level_1,
	n.level_2,
	n.level_3,
	n.level_4,
	n.good_title,
	n.img_url,
	n.grid_url,
	n.thumb_url,
	n.thumb_extend_url,
	n.lang,
	n.stock_qty,
	n.avg_score,
	n.total_num,
	n.total_favorite,
	n.pipeline_code,
	n.url_title
FROM
	dw_gearbest_recommend.goods_info_result_uniq n
JOIN dw_gearbest_recommend.pipeline_language_map x ON n.pipeline_code = x.pipeline_code
AND n.lang = x.lang
WHERE
	n.good_sn NOT IN (
		SELECT
			t.sku
		FROM
			(
				SELECT
					sku
				FROM
					dw_gearbest_recommend.gb_black_gray_result
				WHERE
					add_time = '${YEAR}-${MONTH}-${DAY}'
				AND flag IN ('black', 'gray')
			) t
               UNION ALL
               SELECT
			x.sku
		FROM
			(
				SELECT b.goods_sn as sku FROM dw_gearbest_recommend.sku_not_sale b				
			) x
	)
;

INSERT OVERWRITE TABLE apl_nodetree_gb_fact
SELECT
    id,
    level_1,
    level_2,
    level_3,
    level_4,
    level_cnt
FROM
   ods.ods_o_gearbest_bigdata_goods_category_level 
WHERE day='${DATE}';	

	
INSERT OVERWRITE TABLE apl_sku_second_catgory_fact
SELECT
    good_sn,
    node2
FROM
    (
    SELECT 
	a.good_sn,    --商品sku
	b.node2        --对应的二级分类
FROM  
	goods_info_mid1 a 
JOIN
	(SELECT 
		cat_id,               --商品本身cat_id
		(CASE WHEN node2='' THEN node1 ELSE node2 END) AS node2  --如果二级分类存在选取该sku对应二级分类，不存在则使用一级分类代替
	FROM  
	apl_nodetree_gb_fact
	)b
ON 
	a.id = b.cat_id
    ) C
GROUP BY
    good_sn,
    node2;
	

--之前从mongodb导入商品分类到dw_gearbest_recommend.goods_category_level，现为兼容以前代码，从ods获取覆盖
INSERT OVERWRITE TABLE dw_gearbest_recommend.goods_category_level
SELECT 
	id, 
	r_path,
	level_1,
	level_2,
	level_3,
	level_4,
	level_5,
	level_cnt 
FROM 
	ods.ods_o_gearbest_bigdata_goods_category_level 
WHERE day='${DATE}';
	
	
INSERT OVERWRITE TABLE apl_sku_third_catgory_fact
SELECT
	good_sn,
	(CASE WHEN node3='' THEN node2 ELSE node3 END) AS node3
FROM
	(
		SELECT 
			a.good_sn,
			a.node2,
			b.node3
		FROM
			apl_sku_second_catgory_fact a
		LEFT JOIN
			(
				SELECT 
					a.good_sn,
					b.node3
				FROM
					goods_info_mid1 a
				LEFT JOIN 
					(
						SELECT
							cat_id,
							node3
						FROM
							apl_nodetree_gb_fact
					)b
				ON
					a.id = b.cat_id
			)b
		ON
			a.good_sn = b.good_sn
	
	)c
;

set hive.auto.convert.join = false;
INSERT OVERWRITE TABLE apl_sku_third_catgory_fact
SELECT
	good_sn,
	node3
FROM
	apl_sku_third_catgory_fact
GROUP BY 
	good_sn,
	node3;
	
	
	
	
INSERT OVERWRITE TABLE apl_sku_first_catgory_fact
SELECT 
	a.good_sn,
	b.node1
FROM
	goods_info_mid1 a
JOIN 
	(
		SELECT
			cat_id,
			node1
		FROM
			apl_nodetree_gb_fact
	)b
ON
	a.id = b.cat_id
WHERE
	a.good_sn NOT IN(
					SELECT
						good_sn
					FROM
						apl_elecig_fact
					)
UNION ALL
	SELECT
		good_sn,
		node2 node1
	FROM
		apl_elecig_fact;
		
		
INSERT OVERWRITE TABLE apl_sku_first_catgory_fact
SELECT
	good_sn,
	node1
FROM
	apl_sku_first_catgory_fact
GROUP BY 
	good_sn,
	node1;
