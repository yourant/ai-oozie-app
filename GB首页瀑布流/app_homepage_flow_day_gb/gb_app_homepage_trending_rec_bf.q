--@author LIUQINGFAN
--@date 2018年8月6日 
--@desc  瀑布流置顶商品

SET mapred.job.name='gb_app_homepage_trending_rec';

set mapred.job.queue.name=root.ai.offline;

USE dw_gearbest_recommend;

CREATE TABLE IF NOT EXISTS result_gb_sale_cat_2week_bf
(
	goods_sn         string  ,
	cat_id           string  ,
	score            double     ,
	platform         string,
	pipeline_code    string
	)
COMMENT "两周销量增长环比"
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\u0001' 
LINES TERMINATED BY '\n' 
STORED AS TEXTFILE;

	
INSERT OVERWRITE TABLE result_gb_sale_cat_2week_bf	
SELECT
	a.goods_sn,
	a.category_id,
	a.qty_1/qty_14,
	'',
	a.pipeline_code
FROM(
	SELECT
		goods_sn,
		d.category_id,
		sum(qty) qty_1,
		b.pipeline_code
	FROM
		stg.gb_order_order_goods a
	JOIN
		(SELECT
			order_sn,
			created_time,
			pipeline_code
		FROM
			stg.gb_order_order_info
		WHERE
			from_unixtime(created_time, 'yyyyMMdd') = ${DATE}
		)b
	ON
		a.order_sn = b.order_sn
	JOIN
		goods_info_mid5 d
	ON
		a.goods_sn = d.good_sn
	group by
		goods_sn,
		d.category_id,
		b.pipeline_code
	)a
JOIN(
	SELECT
		goods_sn,
		d.category_id,
		sum(qty) qty_14,
		b.pipeline_code
	FROM
		stg.gb_order_order_goods a
	JOIN
		(SELECT
			order_sn,
			created_time,
			pipeline_code
		FROM
			stg.gb_order_order_info
		WHERE
			from_unixtime(created_time, 'yyyyMMdd') > ${DAY2WEEK}
		)b
	ON
		a.order_sn = b.order_sn
	JOIN
		goods_info_mid5 d
	ON
		a.goods_sn = d.good_sn
	group by
		goods_sn,
		d.category_id,
		b.pipeline_code
	having
		qty_14 > 28
	)b
ON
	a.goods_sn = b.goods_sn and a.category_id = b.category_id and a.pipeline_code = b.pipeline_code
JOIN
	(SELECT
		goods_sn,
		d.category_id,
		sum(qty) qty_1,
		b.pipeline_code
	FROM
		stg.gb_order_order_goods a
	JOIN
		(SELECT
			order_sn,
			created_time,
			pipeline_code
		FROM
			stg.gb_order_order_info
		WHERE
			from_unixtime(created_time, 'yyyyMMdd') = ${DAYWEEK}
		)b
	ON
		a.order_sn = b.order_sn
	JOIN
		goods_info_mid5 d
	ON
		a.goods_sn = d.good_sn
	group by
		goods_sn,
		d.category_id,
		b.pipeline_code
	HAVING
		qty_1 > 1
	)c
ON
	a.goods_sn = c.goods_sn and a.category_id = c.category_id  and a.pipeline_code = c.pipeline_code;
	
	
	

CREATE TABLE IF NOT EXISTS result_gb_cat_sku_500_bf
(
	goods_spu         string  ,
	goods_sn          string,
	cat_id            string  ,
	score             double   ,
	platform          string,
	pipeline_code     string
	)
COMMENT "分类下商品汇总"
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\u0001' 
LINES TERMINATED BY '\n' 
STORED AS TEXTFILE;	
	
	
INSERT OVERWRITE TABLE result_gb_cat_sku_500_bf
SELECT
	goods_spu,
	good_sn,
	id,
	0,
	'',
	pipeline_code
FROM(
	SELECT
		b.goods_spu,
		b.good_sn,
		id,
		pipeline_code,
		ROW_NUMBER() OVER(PARTITION BY id,pipeline_code ORDER BY b.good_sn asc) AS flag
	FROM(
		SELECT	
			goods_spu,
			id,
			pipeline_code
		FROM
			goods_info_mid1
		group BY
			goods_spu,
			id,
			pipeline_code
		)a
	JOIN
		goods_info_mid5 b
	ON
		a.goods_spu = b.goods_spu
	)a
WHERE
	flag < 401;
	
	
	
CREATE TABLE IF NOT EXISTS result_gb_cat_sku_app_mid_bf
(
	goods_spu         string  ,
	goods_sn          string,
	cat_id            string  , 
	score             double     ,
	platform          string,
	pipeline_code     string,
	flag              int
	)
COMMENT "分类下商品汇总"
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\u0001' 
LINES TERMINATED BY '\n' 
STORED AS TEXTFILE;	

INSERT OVERWRITE TABLE 	result_gb_cat_sku_app_mid_bf
SELECT
	goods_spu,
	goods_sn,
	cat_id,
	score,
	platform,
	pipeline_code,
	ROW_NUMBER() OVER(PARTITION BY cat_id,pipeline_code ORDER BY score DESC) AS flag
FROM(
	SELECT
		goods_spu,
		goods_sn,
		cat_id,
		score,
		platform,
		pipeline_code,
		ROW_NUMBER() OVER(PARTITION BY cat_id,pipeline_code,goods_spu ORDER BY score DESC) AS flag
	FROM(
		SELECT
			goods_spu,
			goods_sn,
			cat_id,
			score,
			platform,
			pipeline_code
		FROM
			result_gb_sale_cat_2week_bf a
		JOIN
			(SELECT
				good_sn,
				goods_spu
			FROM
				goods_info_result_rec
			GROUP BY
				good_sn,
				goods_spu
			)b
		ON
			a.goods_sn = b.good_sn
		UNION ALL
		SELECT
			goods_spu       ,
			goods_sn        ,
			cat_id          ,
			score           ,
			platform        ,
			pipeline_code   
		FROM
			result_gb_cat_sku_500_bf
		)a
	)tmp
WHERE
	flag = 1;
	
	
CREATE TABLE IF NOT EXISTS result_gb_cat_sku_app_bf
(
	goods_spu         string  ,
	goods_sn          string,
	cat_id            int  , 
	score             double     ,
	platform          string,
	flag              int,
	pipeline_code     string,
	lang              string,
	brandCode         string,
	brandName         string,
	catId             int,
	catName           string
	)
COMMENT "分类下商品汇总"
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\u0001' 
LINES TERMINATED BY '\n' 
STORED AS TEXTFILE;	

INSERT OVERWRITE TABLE result_gb_cat_sku_app_bf
SELECT
	t1.goods_spu,
	goods_sn,
	t1.cat_id,
	score,
	platform,
	flag,
	pipeline_code,
	t3.lang,
	t5.brand_code,
	t5.brand_code,
	t2.node1,
	t3.cat_name
FROM
	result_gb_cat_sku_app_mid_bf t1
JOIN
	apl_nodetree_gb_fact t2
ON
	t1.cat_id = t2.cat_id
LEFT JOIN
	stg.gb_goods_site_multi_lang_category t3
ON
	t2.node1 = t3.category_id
LEFT JOIN(
	SELECT
		good_sn,
		brand_code
	FROM
		stg.gb_goods_goods_info 
	GROUP BY
		good_sn,
		brand_code
	)t4
ON
	t1.goods_sn = t4.good_sn
LEFT JOIN(
	SELECT
		lang,
		brand_code,
		brand_name
	FROM
		site_multi_lang_brand
	WHERE
		is_use = 1
	) t5
ON
	t4.brand_code = t5.brand_code and t3.lang = t5.lang;
	
INSERT OVERWRITE TABLE result_gb_cat_sku_app_bf
SELECT
	goods_spu       ,
	goods_sn        ,
	cat_id          ,
	score           ,
	platform        ,
	flag            ,
	t1.pipeline_code   ,
	t2.lang			,
	brandCode       ,
	brandName       ,
	catId           ,
	catName     
from(
	SELECT
		goods_spu       ,
		goods_sn        ,
		cat_id          ,
		score           ,
		platform        ,
		flag            ,
		pipeline_code   ,
		CASE lang
		WHEN	 'en-gb' then 'en'
		WHEN	 'en-us' then 'en'
		ELSE
			lang END AS lang,	
		brandCode       ,
		brandName       ,
		catId           ,
		catName         
	FROM
		result_gb_cat_sku_app_bf
	)t1
JOIN
	pipeline_language_map t2
ON
	t1.pipeline_code = t2.pipeline_code and t1.lang = t2.lang	
;


INSERT OVERWRITE TABLE gb_app_homepage_result_bf 
SELECT
	t1.id tab_id,
	t3.good_sn,
	t3.pipeline_code,
	t3.goods_web_sku,
	t3.good_title,
	t3.id cat_id,
	t3.lang,
	t3.v_wh_code,
	t3.total_num,
	t3.avg_score,
	t3.shop_price,
	t3.total_favorite,
	t3.stock_qty,
	t3.img_url,
	t3.grid_url,
	t3.thumb_url,
	t3.thumb_extend_url,
	t3.url_title,
	t1.platform,
	t1.brandCode,
	t1.brandName,
	t1.catId,
	t1.catName,
	t1.flag
FROM(
	SELECT
		id,
		is_show,
		a.pipeline_code,
		pl platform,
		b.cat_ids,
		c.score,
		c.goods_sn,
		c.lang,
		c.brandCode,
		c.brandName,
		c.catId,
		c.catName,
		ROW_NUMBER() OVER(PARTITION BY id,pl,lang,a.pipeline_code,pl ORDER BY flag ASC,score DESC ) AS flag
	FROM(
		SELECT
			id,
			is_show,
			pipeline_code,
			pl,
			label
		FROM
			app_index_tab
				lateral view explode(split(interest_labels, ',')) myTable as label
				lateral view explode(split(platforms, ',')) myTable as pl		
		WHERE
			is_show = 1
		)a
	JOIN(
		SELECT
			label_id,
			cat_ids
		FROM 
			interest 	
				lateral view explode(split(cat_id, ',')) myTable as cat_ids
		)b
	ON
		a.label = b.label_id
	JOIN
		result_gb_cat_sku_app_bf c
	ON
		b.cat_ids = c.cat_id and a.pipeline_code = c.pipeline_code
	)t1
JOIN(
	select
		t3.good_sn,
		t3.pipeline_code,
		t3.goods_web_sku,
		t3.good_title,
		t3.id ,
		t3.lang,
		t3.v_wh_code,
		t3.total_num,
		t3.avg_score,
		t3.shop_price,
		t3.total_favorite,
		t3.stock_qty,
		t3.img_url,
		t3.grid_url,
		t3.thumb_url,
		t3.thumb_extend_url,
		t3.url_title
	FROM
		goods_info_result_uniq t3
	join
		pipeline_language_map t4
	ON
		t3.pipeline_code = t4.pipeline_code AND t4.lang = t3.lang
	)t3
ON
	t1.pipeline_code = t3.pipeline_code AND t1.lang = t3.lang and t1.goods_sn = t3.good_sn;
	
	
INSERT OVERWRITE TABLE gb_app_homepage_result_bf
SELECT
	tab_id,
	good_sn,
	pipeline_code,
	goods_web_sku,
	good_title,
	cat_id,
	lang,
	v_wh_code ,
	total_num,
	avg_score,
	shop_price,
	total_favorite,
	stock_qty,
	img_url,
	grid_url,
	thumb_url,
	thumb_extend_url,
	url_title,
	platform,
	brandCode,
	brandName,
	catId,
	catName,
	flags
FROM(
	SELECT
		tab_id,
		good_sn,
		pipeline_code,
		goods_web_sku,
		good_title,
		cat_id,
		lang,
		v_wh_code,
		total_num,
		avg_score,
		shop_price,
		total_favorite,
		stock_qty,
		img_url,
		grid_url,
		thumb_url,
		thumb_extend_url,
		url_title,
		platform,
		brandCode,
		brandName,
		catId,
		catName,
		ROW_NUMBER() OVER(PARTITION BY tab_id,platform,lang,pipeline_code ORDER BY flag ASC) AS flags
	FROM
		gb_app_homepage_result_bf
	)tmp
WHERE
	flags < 701;
	
	
	
	
CREATE TABLE IF NOT EXISTS gb_app_homepage_trending_bf(
	tab_id             int        ,
	good_sn            string     COMMENT '推荐商品SKU',
	pipeline_code      string     COMMENT '渠道编码',
	webGoodSn          string     COMMENT '商品webSku',
	goodsTitle         string     COMMENT '商品title',
	catid              int        COMMENT '商品分类ID',
	lang               string     COMMENT '语言',
	wareCode           int        COMMENT '仓库',
	reviewCount        int        COMMENT '评论数',
	avgRate            double     COMMENT '评论分',
	shopPrice          double     COMMENT '本店售价',
	favoriteCount      int 	      COMMENT '收藏数量',
	goodsNum           int        COMMENT '商品库存',
	imgUrl             string     COMMENT '产品图url',
	gridUrl            string     COMMENT 'grid图url',
	thumbUrl           string     COMMENT '缩略图url',
	thumbExtendUrl     string     COMMENT '商品图片',
	url_title          string     COMMENT '静态页面文件标题',
	platform           int,
	brandCode          string,
	brandName          string,
	catIdTop           int,
	catName            string,
	score               int
	)
COMMENT ''
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\u0001'                                                                                   
LINES TERMINATED BY '\n'                                                                                          
STORED AS TEXTFILE;


INSERT OVERWRITE TABLE gb_app_homepage_trending_bf
SELECT
	tab_id,
	good_sn,
	pipeline_code,
	goods_web_sku,
	good_title,
	cat_id,
	lang,
	v_wh_code warecode,
	total_num,
	avg_score,
	shop_price,
	total_favorite,
	stock_qty,
	img_url,
	grid_url,
	thumb_url,
	thumb_extend_url,
	url_title,
	platform,
	brandCode,
	brandName,
	catId,
	catName,
	flag score
FROM
	gb_app_homepage_result_bf
WHERE
	(0< flag and flag < 120) OR (flag > 160);
	

INSERT OVERWRITE TABLE gb_app_homepage_trending_bf
SELECT
	tab_id      ,
	good_sn         ,
	pipeline_code   ,
	webGoodSn       ,
	goodsTitle      ,
	catid           ,
	lang            ,
	wareCode        ,
	reviewCount     ,
	avgRate         ,
	shopPrice       ,
	favoriteCount   ,
	goodsNum        ,
	imgUrl          ,
	gridUrl         ,
	thumbUrl        ,
	thumbExtendUrl  ,
	url_title       ,
	platform        ,
	brandCode       ,
	brandName       ,
	catIdTop        ,
	catName         ,
	score           
FROM(
	SELECT
		tab_id      ,
		good_sn         ,
		pipeline_code   ,
		webGoodSn       ,
		goodsTitle      ,
		catid           ,
		lang            ,
		wareCode        ,
		reviewCount     ,
		avgRate         ,
		shopPrice       ,
		favoriteCount   ,
		goodsNum        ,
		imgUrl          ,
		gridUrl         ,
		thumbUrl        ,
		thumbExtendUrl  ,
		url_title       ,
		platform        ,
		brandCode       ,
		brandName       ,
		catIdTop        ,
		catName         ,
		ROW_NUMBER() OVER(PARTITION BY tab_id,platform,lang,pipeline_code ORDER BY score ASC) AS score
	FROM
		gb_app_homepage_trending_bf
	)tmp;

INSERT OVERWRITE TABLE result_gb_cat_sku_app_popular_bf
SELECT
	goods_sn        ,
	cat_id          ,
	score           ,
	'2'    platform    ,
	flag            ,
	pipeline_code   ,
	lang            ,
	brandCode       ,
	brandName       ,
	catId           ,
	catName        
FROM(
	SELECT
		goods_sn        ,
		cat_id          ,
		score           ,
		'2'       		,
		flag            ,
		pipeline_code   ,
		lang            ,
		brandCode       ,
		brandName       ,
		catId           ,
		catName        ,
		ROW_NUMBER() OVER(PARTITION BY pipeline_code,cat_id,lang ORDER BY goods_sn ASC) AS SCORES
	FROM
		result_gb_cat_sku_app_bf
	)tmp
WHERE
	SCORES < 10
UNION ALL
SELECT
	goods_sn        ,
	cat_id          ,
	score           ,
	'3'       platform ,
	flag            ,
	pipeline_code   ,
	lang            ,
	brandCode       ,
	brandName       ,
	catId           ,
	catName        
FROM(
	SELECT
		goods_sn        ,
		cat_id          ,
		score           ,
		'3'       		,
		flag            ,
		pipeline_code   ,
		lang            ,
		brandCode       ,
		brandName       ,
		catId           ,
		catName        ,
		ROW_NUMBER() OVER(PARTITION BY pipeline_code,cat_id,lang ORDER BY goods_sn ASC) AS SCORES
	FROM
		result_gb_cat_sku_app_bf
	)tmp
WHERE
	SCORES < 10
UNION ALL
SELECT
	goods_sn        ,
	cat_id          ,
	score           ,
	'4'       platform ,
	flag            ,
	pipeline_code   ,
	lang            ,
	brandCode       ,
	brandName       ,
	catId           ,
	catName        
FROM(
	SELECT
		goods_sn        ,
		cat_id          ,
		score           ,
		'4'       		,
		flag            ,
		pipeline_code   ,
		lang            ,
		brandCode       ,
		brandName       ,
		catId           ,
		catName        ,
		ROW_NUMBER() OVER(PARTITION BY pipeline_code,cat_id,lang ORDER BY goods_sn ASC) AS SCORES
	FROM
		result_gb_cat_sku_app_bf
	)tmp
WHERE
	SCORES < 10;


CREATE TABLE IF NOT EXISTS gb_cat_sku_app_popular_result_bf(
	tab_id             int        ,
	good_sn            string     COMMENT '推荐商品SKU',
	pipeline_code      string     COMMENT '渠道编码',
	webGoodSn          string     COMMENT '商品webSku',
	goodsTitle         string     COMMENT '商品title',
	catid              int        COMMENT '商品分类ID',
	lang               string     COMMENT '语言',
	wareCode           int        COMMENT '仓库',
	reviewCount        int        COMMENT '评论数',
	avgRate            double     COMMENT '评论分',
	shopPrice          double     COMMENT '本店售价',
	favoriteCount      int 	      COMMENT '收藏数量',
	goodsNum           int        COMMENT '商品库存',
	imgUrl             string     COMMENT '产品图url',
	gridUrl            string     COMMENT 'grid图url',
	thumbUrl           string     COMMENT '缩略图url',
	thumbExtendUrl     string     COMMENT '商品图片',
	url_title          string     COMMENT '静态页面文件标题',
	platform           int,
	brandCode          string,
	brandName          string,
	catIdTop           int,
	catName            string,
	score               int
	)
COMMENT ''
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\u0001'                                                                                   
LINES TERMINATED BY '\n'                                                                                          
STORED AS TEXTFILE;	
	
INSERT OVERWRITE TABLE gb_cat_sku_app_popular_result_bf
SELECT
	000000,
	t3.good_sn,
	t3.pipeline_code,
	t3.goods_web_sku,
	t3.good_title,
	t3.id cat_id,
	t3.lang,
	t3.v_wh_code,
	t3.total_num,
	t3.avg_score,
	t3.shop_price,
	t3.total_favorite,
	t3.stock_qty,
	t3.img_url,
	t3.grid_url,
	t3.thumb_url,
	t3.thumb_extend_url,
	t3.url_title,
	t1.platform,
	t1.brandCode,
	t1.brandName,
	t1.catId,
	t1.catName,
	t1.flag score
FROM(
	SELECT
		goods_sn        ,
		cat_id          ,
		score           ,
		platform       ,
		pipeline_code   ,
		lang            ,
		brandCode       ,
		brandName       ,
		catId           ,
		catName         ,
		ROW_NUMBER() OVER(PARTITION BY pipeline_code,platform,lang ORDER BY rand() DESC ) AS flag
	FROM
		result_gb_cat_sku_app_popular_bf
	) t1
JOIN(
	select
		t3.good_sn,
		t3.pipeline_code,
		t3.goods_web_sku,
		t3.good_title,
		t3.id ,
		t3.lang,
		t3.v_wh_code,
		t3.total_num,
		t3.avg_score,
		t3.shop_price,
		t3.total_favorite,
		t3.stock_qty,
		t3.img_url,
		t3.grid_url,
		t3.thumb_url,
		t3.thumb_extend_url,
		t3.url_title
	FROM
		goods_info_result_uniq t3
	join
		pipeline_language_map t4
	ON
		t3.pipeline_code = t4.pipeline_code AND t4.lang = t3.lang
	)t3
ON
	t1.pipeline_code = t3.pipeline_code AND t1.lang = t3.lang and t1.goods_sn = t3.good_sn
WHERE
	t1.flag < 701;

CREATE TABLE IF NOT EXISTS gb_app_homepage_trending_rec_bf(
	tab_id             int        ,
	good_sn            string     COMMENT '推荐商品SKU',
	pipeline_code      string     COMMENT '渠道编码',
	webGoodSn          string     COMMENT '商品webSku',
	goodsTitle         string     COMMENT '商品title',
	catid              int        COMMENT '商品分类ID',
	lang               string     COMMENT '语言',
	wareCode           int        COMMENT '仓库',
	reviewCount        int        COMMENT '评论数',
	avgRate            double     COMMENT '评论分',
	shopPrice          double     COMMENT '本店售价',
	favoriteCount      int 	      COMMENT '收藏数量',
	goodsNum           int        COMMENT '商品库存',
	originalUrl        string     COMMENT '商品原图',
	imgUrl             string     COMMENT '产品图url',
	gridUrl            string     COMMENT 'grid图url',
	thumbUrl           string     COMMENT '缩略图url',
	thumbExtendUrl     string     COMMENT '商品图片',
	url_title          string     COMMENT '静态页面文件标题',
	platform           int,
	brandCode          string,
	brandName          string,
	catIdTop           int,
	catName            string,
	score               int
	)
COMMENT ''
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\u0001'                                                                                   
LINES TERMINATED BY '\n'                                                                                          
STORED AS TEXTFILE;

INSERT OVERWRITE TABLE gb_app_homepage_trending_rec_bf
SELECT
	t1.tab_id      ,
	t1.good_sn         ,
	t1.pipeline_code   ,
	t1.webGoodSn       ,
	t1.goodsTitle      ,
	t1.catid           ,
	t1.lang            ,
	t1.wareCode        ,
	t1.reviewCount     ,
	t1.avgRate         ,
	t1.shopPrice       ,
	t1.favoriteCount   ,
	t1.goodsNum        ,
	t2.original_url,
	t1.imgUrl          ,
	t1.gridUrl         ,
	t1.thumbUrl        ,
	t1.thumbExtendUrl  ,
	t1.url_title       ,
	t1.platform        ,
	t1.brandCode       ,
	t1.brandName       ,
	t1.catIdTop        ,
	t1.catName         ,
	t1.score
FROM(
	SELECT
		tab_id      ,
		good_sn         ,
		pipeline_code   ,
		webGoodSn       ,
		goodsTitle      ,
		catid           ,
		lang            ,
		wareCode        ,
		reviewCount     ,
		avgRate         ,
		shopPrice       ,
		favoriteCount   ,
		goodsNum        ,
		imgUrl          ,
		gridUrl         ,
		thumbUrl        ,
		thumbExtendUrl  ,
		url_title       ,
		platform        ,
		brandCode       ,
		brandName       ,
		catIdTop        ,
		catName         ,
		ROW_NUMBER() OVER(PARTITION BY tab_id,platform,lang,pipeline_code ORDER BY score ASC) AS score
	FROM
		gb_app_homepage_trending_bf	
	UNION ALL
	SELECT
		tab_id      ,
		good_sn         ,
		pipeline_code   ,
		webGoodSn       ,
		goodsTitle      ,
		catid           ,
		lang            ,
		wareCode        ,
		reviewCount     ,
		avgRate         ,
		shopPrice       ,
		favoriteCount   ,
		goodsNum        ,
		imgUrl          ,
		gridUrl         ,
		thumbUrl        ,
		thumbExtendUrl  ,
		url_title       ,
		platform        ,
		brandCode       ,
		brandName       ,
		catIdTop        ,
		catName         ,
		ROW_NUMBER() OVER(PARTITION BY tab_id,platform,lang,pipeline_code ORDER BY score ASC) AS score
	FROM
		gb_cat_sku_app_popular_result_bf
	)t1
JOIN(
	SELECT
		good_sn,
		original_url
	FROM
		goods_info_mid2
	GROUP BY
		good_sn,
		original_url
	)t2
ON
	t1.good_sn = t2.good_sn
	;
	
	
INSERT OVERWRITE TABLE gb_app_homepage_trending_rec_bf
SELECT
	tab_id      ,
	good_sn         ,
	pipeline_code   ,
	webGoodSn       ,
	goodsTitle      ,
	catid           ,
	lang            ,
	wareCode        ,
	reviewCount     ,
	avgRate         ,
	shopPrice       ,
	favoriteCount   ,
	goodsNum        ,
	originalUrl,
	imgUrl          ,
	gridUrl         ,
	thumbUrl        ,
	thumbExtendUrl  ,
	url_title       ,
	platform        ,
	brandCode       ,
	brandName       ,
	catIdTop        ,
	catName         ,
	score
FROM(SELECT
	tab_id      ,
	good_sn         ,
	pipeline_code   ,
	webGoodSn       ,
	goodsTitle      ,
	catid           ,
	lang            ,
	wareCode        ,
	reviewCount     ,
	avgRate         ,
	shopPrice       ,
	favoriteCount   ,
	goodsNum        ,
	originalUrl,
	imgUrl          ,
	gridUrl         ,
	thumbUrl        ,
	thumbExtendUrl  ,
	url_title       ,
	platform        ,
	brandCode       ,
	brandName       ,
	catIdTop        ,
	catName         ,
	score,
	ROW_NUMBER() OVER(PARTITION BY tab_id,platform,lang,pipeline_code,good_sn ORDER BY brandCode DESC) AS flag
FROM
	gb_app_homepage_trending_rec_bf)t
WHERE
	flag = 1;
	

	

INSERT OVERWRITE TABLE gb_app_homepage_cat_name_bf
SELECT
	tab_id,
	pipeline_code,
	catId,
	catName,
	platform,
	lang,
	ROW_NUMBER() OVER(PARTITION BY tab_id,platform,lang,pipeline_code ORDER BY rand() ) AS score
FROM(
	SELECT
		tab_id,
		pipeline_code,
		catIdTop catId,
		catName,
		platform,
		lang
	FROM
		gb_app_homepage_trending_rec_bf
	GROUP BY
		tab_id,
		pipeline_code,
		catIdTop,
		catName,
		platform,
		lang
	)tmp;
	
INSERT OVERWRITE TABLE gb_app_homepage_brand_name_bf
SELECT
	tab_id,
	pipeline_code,
	brandCode,
	brandName,
	platform,
	lang,
	ROW_NUMBER() OVER(PARTITION BY tab_id,platform,lang,pipeline_code ORDER BY rand() ) AS score
FROM(
	SELECT
		tab_id,
		pipeline_code,
		brandCode,
		brandName,
		platform,
		lang
	FROM
		gb_app_homepage_trending_rec_bf
	WHERE
		brandName is not NULL and brandCode is not NULL
	GROUP BY
		tab_id,
		pipeline_code,
		brandCode,
		brandName,
		platform,
		lang
	)tmp;





	
CREATE TABLE IF NOT EXISTS gb_app_homepage_trending_thirty_bf(
	tab_id             int        ,
	good_sn            string     COMMENT '推荐商品SKU',
	pipeline_code      string     COMMENT '渠道编码',
	webGoodSn          string     COMMENT '商品webSku',
	goodsTitle         string     COMMENT '商品title',
	catid              int        COMMENT '商品分类ID',
	lang               string     COMMENT '语言',
	wareCode           int        COMMENT '仓库',
	reviewCount        int        COMMENT '评论数',
	avgRate            double     COMMENT '评论分',
	shopPrice          double     COMMENT '本店售价',
	favoriteCount      int 	      COMMENT '收藏数量',
	goodsNum           int        COMMENT '商品库存',
	imgUrl             string     COMMENT '产品图url',
	gridUrl            string     COMMENT 'grid图url',
	thumbUrl           string     COMMENT '缩略图url',
	thumbExtendUrl     string     COMMENT '商品图片',
	url_title          string     COMMENT '静态页面文件标题',
	platform           int,
	brandCode          string,
	brandName          string,
	catIdTop           int,
	catName            string,
	score              int
	)
COMMENT ''
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\u0001'                                                                                   
LINES TERMINATED BY '\n'                                                                                          
STORED AS TEXTFILE;

INSERT OVERWRITE TABLE gb_app_homepage_trending_thirty_bf
SELECT
	tab_id,
	good_sn,
	pipeline_code,
	goods_web_sku,
	good_title,
	cat_id,
	lang,
	v_wh_code warecode,
	total_num,
	avg_score,
	shop_price,
	total_favorite,
	stock_qty,
	img_url,
	grid_url,
	thumb_url,
	thumb_extend_url,
	url_title,
	platform,
	brandCode,
	brandName,
	catId,
	catName,
	flag score
FROM
	gb_app_homepage_result_bf
WHERE
	(flag BETWEEN 120 AND 160) OR (flag BETWEEN 600 AND 640);
	
INSERT OVERWRITE TABLE gb_app_homepage_trending_thirty_bf
SELECT
	tab_id      ,
	good_sn         ,
	pipeline_code   ,
	webGoodSn       ,
	goodsTitle      ,
	catid           ,
	lang            ,
	wareCode        ,
	reviewCount     ,
	avgRate         ,
	shopPrice       ,
	favoriteCount   ,
	goodsNum        ,
	imgUrl          ,
	gridUrl         ,
	thumbUrl        ,
	thumbExtendUrl  ,
	url_title       ,
	platform        ,
	brandCode       ,
	brandName       ,
	catIdTop        ,
	catName         ,
	score
FROM
	gb_app_homepage_trending_thirty_bf
UNION ALL
SELECT
	tab_id      ,
	good_sn         ,
	pipeline_code   ,
	webGoodSn       ,
	goodsTitle      ,
	catid           ,
	lang            ,
	wareCode        ,
	reviewCount     ,
	avgRate         ,
	shopPrice       ,
	favoriteCount   ,
	goodsNum        ,
	imgUrl          ,
	gridUrl         ,
	thumbUrl        ,
	thumbExtendUrl  ,
	url_title       ,
	platform        ,
	brandCode       ,
	brandName       ,
	catIdTop        ,
	catName         ,
	score
FROM(
	SELECT
		tab_id      ,
		good_sn         ,
		pipeline_code   ,
		webGoodSn       ,
		goodsTitle      ,
		catid           ,
		lang            ,
		wareCode        ,
		reviewCount     ,
		avgRate         ,
		shopPrice       ,
		favoriteCount   ,
		goodsNum        ,
		imgUrl          ,
		gridUrl         ,
		thumbUrl        ,
		thumbExtendUrl  ,
		url_title       ,
		platform        ,
		brandCode       ,
		brandName       ,
		catIdTop        ,
		catName         ,
		score,
		ROW_NUMBER() OVER(PARTITION BY tab_id,platform,lang,pipeline_code ORDER BY rand() ) AS scores
	FROM
		gb_cat_sku_app_popular_result_bf
	)tmp
WHERE
	scores < 31
;
	
CREATE TABLE IF NOT EXISTS gb_app_homepage_trending_thirty_rec_bf(
	tab_id             int        ,
	good_sn            string     COMMENT '推荐商品SKU',
	pipeline_code      string     COMMENT '渠道编码',
	webGoodSn          string     COMMENT '商品webSku',
	goodsTitle         string     COMMENT '商品title',
	catid              int        COMMENT '商品分类ID',
	lang               string     COMMENT '语言',
	wareCode           int        COMMENT '仓库',
	reviewCount        int        COMMENT '评论数',
	avgRate            double     COMMENT '评论分',
	shopPrice          double     COMMENT '本店售价',
	favoriteCount      int 	      COMMENT '收藏数量',
	goodsNum           int        COMMENT '商品库存',
	originalUrl        string     COMMENT '商品原图',
	imgUrl             string     COMMENT '产品图url',
	gridUrl            string     COMMENT 'grid图url',
	thumbUrl           string     COMMENT '缩略图url',
	thumbExtendUrl     string     COMMENT '商品图片',
	url_title          string     COMMENT '静态页面文件标题',
	platform           int,
	brandCode          string,
	brandName          string,
	catIdTop           int,
	catName            string,
	score              int
	)
COMMENT ''
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\u0001'                                                                                   
LINES TERMINATED BY '\n'                                                                                          
STORED AS TEXTFILE;
	

INSERT OVERWRITE TABLE gb_app_homepage_trending_thirty_rec_bf
SELECT
	t1.tab_id      ,
	t1.good_sn         ,
	t1.pipeline_code   ,
	t1.webGoodSn       ,
	t1.goodsTitle      ,
	t1.catid           ,
	t1.lang            ,
	t1.wareCode        ,
	t1.reviewCount     ,
	t1.avgRate         ,
	t1.shopPrice       ,
	t1.favoriteCount   ,
	t1.goodsNum        ,
	t2.original_url,
	t1.imgUrl          ,
	t1.gridUrl         ,
	t1.thumbUrl        ,
	t1.thumbExtendUrl  ,
	t1.url_title       ,
	t1.platform        ,
	t1.brandCode       ,
	t1.brandName       ,
	t1.catIdTop        ,
	t1.catName         ,
	t1.score           
FROM(
	SELECT
		tab_id      ,
		good_sn         ,
		pipeline_code   ,
		webGoodSn       ,
		goodsTitle      ,
		catid           ,
		lang            ,
		wareCode        ,
		reviewCount     ,
		avgRate         ,
		shopPrice       ,
		favoriteCount   ,
		goodsNum        ,
		imgUrl          ,
		gridUrl         ,
		thumbUrl        ,
		thumbExtendUrl  ,
		url_title       ,
		platform        ,
		brandCode       ,
		brandName       ,
		catIdTop        ,
		catName         ,
		ROW_NUMBER() OVER(PARTITION BY tab_id,platform,lang,pipeline_code ORDER BY score ASC) AS score
	FROM
		gb_app_homepage_trending_thirty_bf
	)t1
JOIN(
	SELECT
		good_sn,
		original_url
	FROM
		goods_info_mid2
	GROUP BY
		good_sn,
		original_url
	)t2
ON
	t1.good_sn = t2.good_sn
join
	bf_good_sn t3
on
	t1.good_sn = t3.bf_good_sn_value
;

INSERT OVERWRITE TABLE gb_app_homepage_trending_rec_bf
SELECT
	tab_id      ,
	good_sn         ,
	pipeline_code   ,
	webGoodSn       ,
	goodsTitle      ,
	catid           ,
	lang            ,
	wareCode        ,
	reviewCount     ,
	avgRate         ,
	shopPrice       ,
	favoriteCount   ,
	goodsNum        ,
	originalUrl,
	imgUrl          ,
	gridUrl         ,
	thumbUrl        ,
	thumbExtendUrl  ,
	url_title       ,
	platform        ,
	brandCode       ,
	brandName       ,
	catIdTop        ,
	catName         ,
	score
FROM
	(SELECT
	t1.tab_id      ,
	t1.good_sn         ,
	t1.pipeline_code   ,
	t1.webGoodSn       ,
	t1.goodsTitle      ,
	t1.catid           ,
	t1.lang            ,
	t1.wareCode        ,
	t1.reviewCount     ,
	t1.avgRate         ,
	t1.shopPrice       ,
	t1.favoriteCount   ,
	t1.goodsNum        ,
	t1.originalUrl,
	t1.imgUrl          ,
	t1.gridUrl         ,
	t1.thumbUrl        ,
	t1.thumbExtendUrl  ,
	t1.url_title       ,
	t1.platform        ,
	t1.brandCode       ,
	t1.brandName       ,
	t1.catIdTop        ,
	t1.catName         ,
	t1.score,
	t2.goods_spu,
	ROW_NUMBER() OVER(PARTITION BY tab_id,platform,lang,t1.pipeline_code,goods_spu ORDER BY t1.good_sn ASC) AS flag
FROM
	gb_app_homepage_trending_rec_bf t1
JOIN
	goods_info_mid5 t2
ON
	t1.good_sn = t2.good_sn
JOIN
	stg_gb_goods.goods_pipeline_relation t6
ON
	t1.good_sn = t6.good_sn AND t1.pipeline_code = t6.pipeline_code) tmp
WHERE
	flag = 1;


	