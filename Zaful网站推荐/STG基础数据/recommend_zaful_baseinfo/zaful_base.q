--@author LIUQINGFAN
--@date 2018年5月9日 
--@desc  ZAFUL推荐基础数据

SET mapred.job.name='apl_recommend_zaful_base_fact';
set mapred.job.queue.name=root.ai.offline; 
SET hive.auto.convert.join=false;
SET mapred.max.split.size=128000000;
SET mapred.min.split.size=128000000;
SET mapred.min.split.size.per.node=128000000;
SET mapred.min.split.size.per.rack=128000000;
SET hive.exec.reducers.bytes.per.reducer = 128000000;
SET hive.merge.mapfiles=false;
SET hive.merge.mapredfiles= false;
SET hive.merge.size.per.task=256000000;
SET hive.exec.parallel = true; 
USE dw_zaful_recommend;
set hive.support.concurrency=false;
CREATE TABLE IF NOT EXISTS apl_sku_lang_mid(
	goods_id            int       COMMENT '商品主键ID',
	goods_title         string    COMMENT '商品title',
	is_lang_show        boolean   COMMENT '多语言上下架标识',
	url_title           string    COMMENT 'URLTITLE',
	lang                string    COMMENT '语言站标识'
	)
COMMENT "SKU多语言中间表"
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\u0001' 
LINES TERMINATED BY '\n' 
STORED AS TEXTFILE;	


INSERT OVERWRITE TABLE apl_sku_lang_mid
SELECT
	 goods_id,
	 goods_title,
	 is_lang_show,
	 url_title,
	 'ar' lang
FROM
	stg.zaful_eload_goods_ar
UNION  ALL
SELECT
	 goods_id,
	 goods_title,
	 is_lang_show,
	 url_title,
	 'es' lang
FROM
	stg.zaful_eload_goods_es
UNION  ALL
SELECT
	 goods_id,
	 goods_title,
	 is_lang_show,
	 url_title,
	 'de' lang
FROM
	stg.zaful_eload_goods_de
UNION  ALL
SELECT
	 goods_id,
	 goods_title,
	 is_lang_show,
	 url_title,
	 'fr' lang
FROM
	stg.zaful_eload_goods_fr
UNION  ALL
SELECT
	 goods_id,
	 goods_title,
	 is_lang_show,
	 url_title,
	 'pt' lang
FROM
	stg.zaful_eload_goods_pt
UNION  ALL
SELECT
	 goods_id,

	 goods_title,
	 is_lang_show,
	 url_title,
	 'it' lang
FROM
	stg.zaful_eload_goods_it
UNION ALL
SELECT
	 goods_id,
	 goods_title,
	 is_lang_show,
	 url_title,
	 'en' lang
FROM
	stg.zaful_eload_goods
UNION ALL
SELECT
	 goods_id,
	 goods_title,
	 is_lang_show,
	 url_title,
	 'id' lang
FROM
	stg.zaful_eload_goods_id;

CREATE TABLE IF NOT EXISTS apl_sku_color_size_fact(
	goods_id            int,
	goods_sn            string,
	goods_color         string,
	goods_size          string,
	lang                string
	)
COMMENT '多语言商品颜色、尺寸属性中间表'
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\u0001'                                                                                   
LINES TERMINATED BY '\n'                                                                                          
STORED AS TEXTFILE;	

INSERT OVERWRITE TABLE apl_sku_color_size_fact
SELECT 
  g.goods_id,
  g.goods_sn,
  IF(
    gam_color.attr_value_lang != '',
    gam_color.attr_value_lang,
    ga_color.attr_value
  ) goods_color,
  IF(
    gam_size.attr_value_lang != '',
    gam_size.attr_value_lang,
    ga_size.attr_value
  ) goods_size ,
  gam_color.lang
FROM
	stg.zaful_eload_goods g 
  LEFT JOIN stg.zaful_eload_goods_attr ga_size 
    ON g.goods_id = ga_size.goods_id 
    AND ga_size.`attr_id` = 7 
  LEFT JOIN stg.zaful_eload_goods_attr ga_color 
    ON g.goods_id = ga_color.goods_id 
    AND ga_color.`attr_id` = 8 
  LEFT JOIN stg.zaful_eload_goods_attr_multi_lang gam_color 
    ON ga_color.attr_id = gam_color.attr_id 
    AND ga_color.attr_value = gam_color.attr_value 
    AND gam_color.`attr_id` = 8 
  LEFT JOIN stg.zaful_eload_goods_attr_multi_lang gam_size 
    ON ga_size.attr_id = gam_size.attr_id 
    AND ga_size.attr_value = gam_size.attr_value 
    AND gam_size.`attr_id` = 7
WHERE
	g.is_on_sale = 1  AND g.is_delete = 0  AND g.goods_number >0  AND gam_color.lang!= ''
UNION ALL
SELECT 
  g.goods_id,
  g.goods_sn,
  ga_color.attr_value goods_color,
  ga_size.attr_value goods_size ,
  'en' lang
FROM
	stg.zaful_eload_goods g 
LEFT JOIN 
	stg.zaful_eload_goods_attr ga_size 
ON 
	g.goods_id = ga_size.goods_id AND ga_size.`attr_id` = 7
LEFT JOIN 
	stg.zaful_eload_goods_attr ga_color 
ON 
	g.goods_id = ga_color.goods_id AND ga_color.`attr_id` = 8 
WHERE
    g.is_on_sale = 1  AND g.is_delete = 0  AND g.goods_number > 0;	
	

	
INSERT OVERWRITE TABLE apl_sku_color_size_fact
SELECT
	goods_id    ,
	goods_sn    ,
	goods_color ,
	goods_size  ,
	lang   
FROM(
	SELECT
		goods_id    ,
		goods_sn    ,
		goods_color ,
		goods_size  ,
		lang   ,
		ROW_NUMBER() OVER(PARTITION BY goods_id,lang ORDER BY rand() ) AS flag
	FROM
		apl_sku_color_size_fact
     
	)tmp
WHERE
	flag = 1;

	
CREATE TABLE IF NOT EXISTS apl_sku_lang_fact(
	goodssn            string     COMMENT '推荐商品SKU',
	goodsid            int        COMMENT '商品ID',
	catid              int        COMMENT '商品分类ID',
	goodstitle         string     COMMENT '商品title',
	gridurl            string     COMMENT 'grid图url',
	pipelinecode       string     COMMENT '渠道编码',
	shopcode           string     COMMENT '店铺ID',
	webgoodSn          string     COMMENT '商品webSku',
	lang               string     COMMENT '语言',
	warecode           int        COMMENT '仓库',
	reviewcount        int        COMMENT '评论数',
	avgrate            double     COMMENT '评论分',
	shopprice          double     COMMENT '本店售价',
	favoritecount      int 	      COMMENT '收藏数量',
	goodsnum           int        COMMENT '商品库存',
	imgurl             string     COMMENT '产品图url',
	thumburl           string     COMMENT '缩略图url',
	thumbextendUrl     string     COMMENT '商品图片',
	urltitle           string     COMMENT '静态页面文件标题'
	)
COMMENT '多语言属性结果'
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\u0001'                                                                                   
LINES TERMINATED BY '\n'                                                                                          
STORED AS TEXTFILE;


INSERT OVERWRITE TABLE apl_sku_lang_fact
SELECT
	b.goods_sn,
	b.goods_id,
	b.cat_id,
	a.goods_title,
	b.goods_grid,
	'',
	'',
	'',
	a.lang,
	'',
	'',
	'',
	'',
	'',
	'',
	b.goods_img,
	b.goods_thumb,
	'',
	a.url_title
FROM
	apl_sku_lang_mid a
JOIN
	stg.zaful_eload_goods b
ON
	a.goods_id = b.goods_id 
WHERE
	a.is_lang_show = 1 AND b.is_on_sale = 1;
	
CREATE TABLE IF NOT EXISTS apl_sku_lang_cz_fact(
	goodssn            string     COMMENT '推荐商品SKU',
	goodsid            int        COMMENT '商品ID',
	catid              int        COMMENT '商品分类ID',
	goodstitle         string     COMMENT '商品title',
	gridurl            string     COMMENT 'grid图url',
	goodscolor         string     COMMENT '商品颜色',
	goodssize          string     COMMENT '商品尺寸',
	pipelinecode       string     COMMENT '渠道编码',
	shopcode           string     COMMENT '店铺ID',
	webgoodSn          string     COMMENT '商品webSku',
	lang               string     COMMENT '语言',
	warecode           int        COMMENT '仓库',
	reviewcount        int        COMMENT '评论数',
	avgrate            double     COMMENT '评论分',
	shopprice          double     COMMENT '本店售价',
	favoritecount      int 	      COMMENT '收藏数量',
	goodsnum           int        COMMENT '商品库存',
	imgurl             string     COMMENT '产品图url',
	thumburl           string     COMMENT '缩略图url',
	thumbextendUrl     string     COMMENT '商品图片',
	urltitle           string     COMMENT '静态页面文件标题'
	)
COMMENT '多语言属性尺寸结果'
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\u0001'                                                                                   
LINES TERMINATED BY '\n'                                                                                          
STORED AS TEXTFILE;

INSERT OVERWRITE TABLE apl_sku_lang_cz_fact
SELECT
	a.goodssn           ,
	a.goodsid           ,
	a.catid             ,
	a.goodstitle        ,
	a.gridurl           ,
	b.goods_color,
	b.goods_size,
	a.pipelinecode      ,
	a.shopcode          ,
	a.webgoodSn         ,
	a.lang              ,
	a.warecode          ,
	a.reviewcount       ,
	a.avgrate           ,
	a.shopprice         ,
	a.favoritecount     ,
	a.goodsnum          ,
	a.imgurl            ,
	a.thumburl          ,
	a.thumbextendUrl    ,
	a.urltitle          
FROM	
	apl_sku_lang_fact a
LEFT JOIN
	apl_sku_color_size_fact b
ON
	a.goodssn = b.goods_sn AND a.lang = b.lang;

CREATE TABLE IF NOT EXISTS apl_nodetree_zf_fact( 
	cat_id      string        COMMENT 'sku分类cat_id',
	cat_name    string        COMMENT '分类名称',
	node1       string        COMMENT '一级分类',
	node2       string        COMMENT '二级分类',
	node3       string        COMMENT '三级分类',
	node4       string        COMMENT '四级分类'	
)
COMMENT '商品等级分类表'
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\u0001'                                                                                   
LINES TERMINATED BY '\n'                                                                                          
STORED AS TEXTFILE;

INSERT OVERWRITE TABLE  apl_nodetree_zf_fact
SELECT 
	cat_id, 
	cat_name,
	split(d.NODE,',')[0] node1, --一级分类
	split(d.NODE,',')[1] node2, --二级分类
	split(d.NODE,',')[2] node3, --三级分类
	split(d.NODE,',')[3] node4  --四级分类
FROM 
    stg.zaful_eload_category d;
	
CREATE TABLE IF NOT EXISTS apl_sku_color_size_mid(
	goods_id            int,
	goods_sn            string,
	goods_color         string,
	goods_size          string
	)
COMMENT '商品颜色、尺寸属性中间表'
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\u0001'                                                                                   
LINES TERMINATED BY '\n'                                                                                          
STORED AS TEXTFILE;


INSERT OVERWRITE TABLE apl_sku_color_size_mid
SELECT 
  g.goods_id,
  g.goods_sn,
  ga_color.attr_value goods_color,
  ga_size.attr_value goods_size 
FROM
	stg.zaful_eload_goods g 
LEFT JOIN 
	stg.zaful_eload_goods_attr ga_size 
ON 
	g.goods_id = ga_size.goods_id AND ga_size.`attr_id` = 7
LEFT JOIN 
	stg.zaful_eload_goods_attr ga_color 
ON 
	g.goods_id = ga_color.goods_id AND ga_color.`attr_id` = 8 
WHERE
    g.is_on_sale = 1  AND g.is_delete = 0  AND g.goods_number > 0;	
	
----上架时间排序
CREATE TABLE IF NOT EXISTS apl_sku_addtime_zf_fact( 
	cat_id         string        COMMENT 'sku分类cat_id',
	goods_sn       string        COMMENT '商品SKU',
	add_time       string        COMMENT '商品上架时间'
)
COMMENT '商品等级分类表'
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\u0001'                                                                                   
LINES TERMINATED BY '\n'                                                                                          
STORED AS TEXTFILE;

INSERT OVERWRITE TABLE apl_sku_addtime_zf_fact
SELECT
	cat_id,
	goods_sn,
	add_time
FROM(
	SELECT
		goods_sn,
		cat_id,
		add_time,
		ROW_NUMBER() OVER(PARTITION BY cat_id ORDER BY add_time desc) AS flag
	FROM
		stg.zaful_eload_goods
	WHERE
		is_on_sale = 1  AND is_delete = 0  AND goods_number > 0
	)tmp
WHERE
	flag < 31;
	
CREATE TABLE IF NOT EXISTS apl_result_newgoods_zaful_fact(
	goodssn            string     COMMENT '推荐商品SKU',
	goodsid            int        COMMENT '商品ID',
	catid              int        COMMENT '商品分类ID',
	goodstitle         string     COMMENT '商品title',
	goodscolor         string     COMMENT '商品颜色',
	goodssize          string     COMMENT '商品尺寸',
	gridurl            string     COMMENT 'grid图url',
	pipelinecode       string     COMMENT '渠道编码',
	shopcode           string     COMMENT '店铺ID',
	webgoodSn          string     COMMENT '商品webSku',
	lang               string     COMMENT '语言',
	warecode           int        COMMENT '仓库',
	reviewcount        int        COMMENT '评论数',
	avgrate            double     COMMENT '评论分',
	shopprice          double     COMMENT '本店售价',
	favoritecount      int 	      COMMENT '收藏数量',
	goodsnum           int        COMMENT '商品库存',
	imgurl             string     COMMENT '产品图url',
	thumburl           string     COMMENT '缩略图url',
	thumbextendUrl     string     COMMENT '商品图片',
	urltitle           string     COMMENT '静态页面文件标题'
	)
COMMENT '新品推荐结果'
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\u0001'                                                                                   
LINES TERMINATED BY '\n'                                                                                          
STORED AS TEXTFILE;

INSERT OVERWRITE TABLE apl_result_newgoods_zaful_fact
SELECT
	goods_sn,
	goods_id,
	cat_id,
	goods_title,
	goods_color,
	goods_size,
	'',
	'',
	'',
	'',
	'en',
	'',
	'',
	'',
	'',
	'',
	'',
	'',
	goods_thumb,
	'',
	''
FROM(
	SELECT
		b.goods_sn,
		b.goods_id,
		b.cat_id,
		b.goods_title,
		c.goods_color,
		c.goods_size,
		'',
		'',
		'',
		'',
		'',
		'',
		'',
		'',
		'',
		'',
		'',
		'',
		b.goods_thumb,
		'',
		'',
		a.add_time
	FROM
		apl_sku_addtime_zf_fact a
	JOIN
		stg.zaful_eload_goods b
	ON
		a.goods_sn = b.goods_sn
	JOIN
		apl_sku_color_size_mid c
	ON
		c.goods_sn = a.goods_sn
	ORDER BY
		a.add_time desc
	)T
;
INSERT OVERWRITE TABLE apl_result_newgoods_zaful_fact
SELECT
	a.goodssn,
	b.goodsid,
	b.catid,
	b.goodstitle,
	c.goods_color,
	c.goods_size,
	b.gridurl,
	'',
	'',
	'',
	b.lang,
	'',
	'',
	'',
	'',
	'',
	'',
	b.imgurl,
	b.thumburl,
	'',
	b.urltitle
FROM
	apl_result_newgoods_zaful_fact a
LEFT JOIN
	apl_sku_lang_fact  b
ON
	a.goodssn = b.goodssn
LEFT JOIN
	apl_sku_color_size_fact c
ON
	a.goodssn = c.goods_sn AND c.lang = b.lang
WHERE
	b.lang != ''
;	
	
INSERT OVERWRITE TABLE  apl_result_newgoods_zaful_fact
SELECT
	NVL(goodssn,'')              ,
	NVL(goodsid,0)              ,
	NVL(catid,0)                ,
	NVL(goodstitle,'')           ,
	NVL(goodscolor,'')           ,
	NVL(goodssize,'')           ,
	NVL(gridurl,'')              ,
	NVL(pipelinecode,'')        ,
	NVL(shopcode,'')             ,
	NVL(webgoodSn,'')            ,
	NVL(lang,'')                 ,
	NVL(warecode,0)             ,
	NVL(reviewcount,0)          ,
	NVL(avgrate,0)              ,
	NVL(shopprice,0)            ,
	NVL(favoritecount,0)        ,
	NVL(goodsnum,0)             ,
	NVL(imgurl,'')               ,
	NVL(thumburl,'')             ,
	NVL(thumbextendUrl,'')       ,
	NVL(urltitle,'')             
FROM
	apl_result_newgoods_zaful_fact;
	
	
	

CREATE TABLE IF NOT EXISTS apl_hotsell_zf_mid( 
	cat_id             string        COMMENT 'sku分类cat_id',
	goods_sn           string        COMMENT '商品SKU',
	goods_number       int           COMMENT '商品销量'
)
COMMENT '商品分类销量中间表'
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\u0001'                                                                                   
LINES TERMINATED BY '\n'                                                                                          
STORED AS TEXTFILE;





INSERT OVERWRITE TABLE apl_hotsell_zf_mid
SELECT
	cat_id,
	goods_sn,
	goods_number	
FROM(
	SELECT
		goods_sn,
		cat_id,
		goods_number,
		ROW_NUMBER() OVER(PARTITION BY cat_id ORDER BY goods_number desc) AS flag
	FROM(
		SELECT
			a.goods_sn,
			a.goods_number,
			b.cat_id
		FROM(
			SELECT
				goods_sn,
				SUM(goods_number) goods_number
			FROM
				stg.zaful_eload_order_goods
			WHERE
				from_unixtime(addtime+8*3600,'yyyyMMdd') > ${WDATE}
			GROUP BY
				goods_sn
			)a
		JOIN
			stg.zaful_eload_goods b
		ON
			a.goods_sn = b.goods_sn
		)tmp
	)tmp
WHERE
	flag < 31;
	
	
CREATE TABLE IF NOT EXISTS apl_result_hotsell_cat_zaful_fact(
	goodssn            string     COMMENT '推荐商品SKU',
	goodsid            int        COMMENT '商品ID',
	catid              int        COMMENT '商品分类ID',
	goodstitle         string     COMMENT '商品title',
	goodscolor         string     COMMENT '商品颜色',
	goodssize          string     COMMENT '商品尺寸',
	gridurl            string     COMMENT 'grid图url',
	pipelinecode      string     COMMENT '渠道编码',
	shopcode           string     COMMENT '店铺ID',
	webgoodSn          string     COMMENT '商品webSku',
	lang               string     COMMENT '语言',
	warecode           int        COMMENT '仓库',
	reviewcount        int        COMMENT '评论数',
	avgrate            double     COMMENT '评论分',
	shopprice          double     COMMENT '本店售价',
	favoritecount      int 	      COMMENT '收藏数量',
	goodsnum           int        COMMENT '商品库存',
	imgurl             string     COMMENT '产品图url',
	thumburl           string     COMMENT '缩略图url',
	thumbextendUrl     string     COMMENT '商品图片',
	urltitle           string     COMMENT '静态页面文件标题'
	)
COMMENT '分类热销推荐结果'
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\u0001'                                                                                   
LINES TERMINATED BY '\n'                                                                                          
STORED AS TEXTFILE;


INSERT OVERWRITE TABLE apl_result_hotsell_cat_zaful_fact
SELECT
	goods_sn,
	goods_id,
	cat_id,
	goods_title,
	goods_color,
	goods_size,
	'',
	'',
	'',
	'',
	'en',
	'',
	'',
	'',
	'',
	'',
	'',
	'',
	goods_thumb,
	'',
	''
FROM(
	SELECT
		b.goods_sn,
		b.goods_id,
		b.cat_id,
		b.goods_title,
		c.goods_color,
		c.goods_size,
		'',
		'',
		'',
		'',
		'',
		'',
		'',
		'',
		'',
		'',
		'',
		'',
		b.goods_thumb,
		'',
		'',
		a.goods_number
	FROM
		apl_hotsell_zf_mid a
	JOIN
		stg.zaful_eload_goods b
	ON
		a.goods_sn = b.goods_sn
	JOIN
		apl_sku_color_size_mid c
	ON
		c.goods_sn = a.goods_sn
	WHERE
		b.is_on_sale = 1  AND b.is_delete = 0  AND b.goods_number > 0
	ORDER BY
		a.goods_number desc
	)T
;
INSERT OVERWRITE TABLE apl_result_hotsell_cat_zaful_fact
SELECT
	a.goodssn,
	b.goodsid,
	b.catid,
	b.goodstitle,
	c.goods_color,
	c.goods_size,
	b.gridurl,
	'',
	'',
	'',
	b.lang,
	'',
	'',
	'',
	'',
	'',
	'',
	b.imgurl,
	b.thumburl,
	'',
	b.urltitle
FROM
	apl_result_hotsell_cat_zaful_fact a
LEFT JOIN
	apl_sku_lang_fact  b
ON
	a.goodssn = b.goodssn
LEFT JOIN
	apl_sku_color_size_fact c
ON
	a.goodssn = c.goods_sn AND c.lang = b.lang
WHERE
	b.lang != ''
;

INSERT OVERWRITE TABLE  apl_result_hotsell_cat_zaful_fact
SELECT
	NVL(goodssn,'')              ,
	NVL(goodsid,0)              ,
	NVL(catid,0)                ,
	NVL(goodstitle,'')           ,
	NVL(goodscolor,'')           ,
	NVL(goodssize,'')           ,
	NVL(gridurl,'')              ,
	NVL(pipelinecode,'')        ,
	NVL(shopcode,'')             ,
	NVL(webgoodSn,'')            ,
	NVL(lang,'')                 ,
	NVL(warecode,0)             ,
	NVL(reviewcount,0)          ,
	NVL(avgrate,0)              ,
	NVL(shopprice,0)            ,
	NVL(favoritecount,0)        ,
	NVL(goodsnum,0)             ,
	NVL(imgurl,'')               ,
	NVL(thumburl,'')             ,
	NVL(thumbextendUrl,'')       ,
	NVL(urltitle,'')             
FROM
	apl_result_hotsell_cat_zaful_fact;
	
	
	
CREATE TABLE IF NOT EXISTS apl_result_hotsell_all_zaful_fact(
	goodssn            string     COMMENT '推荐商品SKU',
	goodsid            int        COMMENT '商品ID',
	catid              int        COMMENT '商品分类ID',
	goodstitle         string     COMMENT '商品title',
	goodscolor         string     COMMENT '商品颜色',
	goodssize          string     COMMENT '商品尺寸',
	gridurl            string     COMMENT 'grid图url',
	pipelinecode      string     COMMENT '渠道编码',
	shopcode           string     COMMENT '店铺ID',
	webgoodSn          string     COMMENT '商品webSku',
	lang               string     COMMENT '语言',
	warecode           int        COMMENT '仓库',
	reviewcount        int        COMMENT '评论数',
	avgrate            double     COMMENT '评论分',
	shopprice          double     COMMENT '本店售价',
	favoritecount      int 	      COMMENT '收藏数量',
	goodsnum           int        COMMENT '商品库存',
	imgurl             string     COMMENT '产品图url',
	thumburl           string     COMMENT '缩略图url',
	thumbextendUrl     string     COMMENT '商品图片',
	urltitle           string     COMMENT '静态页面文件标题'
	)
COMMENT '网站热销推荐结果'
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\u0001'                                                                                   
LINES TERMINATED BY '\n'                                                                                          
STORED AS TEXTFILE;


INSERT OVERWRITE TABLE apl_result_hotsell_all_zaful_fact
SELECT
	goods_sn,
	goods_id,
	cat_id,
	goods_title,
	goods_color,
	goods_size,
	'',
	'',
	'',
	'',
	'en',
	'',
	'',
	'',
	'',
	'',
	'',
	'',
	goods_thumb,
	'',
	''
FROM(
	SELECT
		b.goods_sn,
		b.goods_id,
		b.cat_id,
		b.goods_title,
		c.goods_color,
		c.goods_size,
		'',
		'',
		'',
		'',
		'',
		'',
		'',
		'',
		'',
		'',
		'',
		'',
		b.goods_thumb,
		'',
		'',
		a.goods_number
	FROM
		(
		SELECT
			goods_sn,
			SUM(goods_number) goods_number
		FROM
			stg.zaful_eload_order_goods
		WHERE
			from_unixtime(addtime+8*3600,'yyyyMMdd') > ${WDATE}
		GROUP BY
			goods_sn
		ORDER BY
			goods_number DESC
		LIMIT 30
		) a
	JOIN
		stg.zaful_eload_goods b
	ON
		a.goods_sn = b.goods_sn
	JOIN
		apl_sku_color_size_mid c
	ON
		c.goods_sn = a.goods_sn
	WHERE
		b.is_on_sale = 1  AND b.is_delete = 0  AND b.goods_number > 0
	ORDER BY
		a.goods_number desc
	)T;
	
	
INSERT OVERWRITE TABLE apl_result_hotsell_all_zaful_fact
SELECT
	a.goodssn,
	b.goodsid,
	b.catid,
	b.goodstitle,
	c.goods_color,
	c.goods_size,
	b.gridurl,
	'',
	'',
	'',
	b.lang,
	'',
	'',
	'',
	'',
	'',
	'',
	b.imgurl,
	b.thumburl,
	'',
	b.urltitle
FROM
	apl_result_hotsell_all_zaful_fact a
LEFT JOIN
	apl_sku_lang_fact  b
ON
	a.goodssn = b.goodssn
LEFT JOIN
	apl_sku_color_size_fact c
ON
	a.goodssn = c.goods_sn AND c.lang = b.lang
WHERE
	b.lang != ''
;
	
	
INSERT OVERWRITE TABLE  apl_result_hotsell_all_zaful_fact
SELECT
	NVL(goodssn,'')              ,
	NVL(goodsid,0)              ,
	NVL(catid,0)                ,
	NVL(goodstitle,'')           ,
	NVL(goodscolor,'')           ,
	NVL(goodssize,'')           ,
	NVL(gridurl,'')              ,
	NVL(pipelinecode,'')        ,
	NVL(shopcode,'')             ,
	NVL(webgoodSn,'')            ,
	NVL(lang,'')                 ,
	NVL(warecode,0)             ,
	NVL(reviewcount,0)          ,
	NVL(avgrate,0)              ,
	NVL(shopprice,0)            ,
	NVL(favoritecount,0)        ,
	NVL(goodsnum,0)             ,
	NVL(imgurl,'')               ,
	NVL(thumburl,'')             ,
	NVL(thumbextendUrl,'')       ,
	NVL(urltitle,'')             
FROM
	apl_result_hotsell_all_zaful_fact;
	

CREATE TABLE IF NOT EXISTS apl_zaful_result_attr_fact(
	goodssn            string     COMMENT '商详页展示商品',
	goodsid            int        COMMENT '商品ID',
	catid              int        COMMENT '商品分类ID',
	goodstitle         string     COMMENT '商品title',
	goodscolor         string     COMMENT '商品颜色',
	goodssize          string     COMMENT '商品尺寸',
	gridurl            string     COMMENT 'grid图url',
	pipelinecode       string     COMMENT '渠道编码',
	shopcode           string     COMMENT '店铺ID',
	webgoodSn          string     COMMENT '商品webSku',
	lang               string     COMMENT '语言',
	warecode           int        COMMENT '仓库',
	reviewcount        int        COMMENT '评论数',
	avgrate            double     COMMENT '评论分',
	shopprice          double     COMMENT '本店售价',
	favoritecount      int 	      COMMENT '收藏数量',
	goodsnum           int        COMMENT '商品库存',
	imgurl             string     COMMENT '产品图url',
	thumburl           string     COMMENT '缩略图url',
	thumbextendUrl     string     COMMENT '商品图片',
	urltitle           string     COMMENT '静态页面文件标题'
	)
COMMENT '商品属性结果'
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\u0001'                                                                                   
LINES TERMINATED BY '\n'                                                                                          
STORED AS TEXTFILE;





INSERT OVERWRITE TABLE apl_zaful_result_attr_fact
SELECT
	goods_sn,
	goods_id,
	cat_id,
	goods_title,
	goods_color,
	goods_size,
	'',
	'',
	'',
	'',
	'en',
	'',
	'',
	'',
	'',
	'',
	'',
	'',
	goods_thumb,
	'',
	''
FROM(
	SELECT
		b.goods_sn ,
		b.goods_id,
		b.cat_id,
		b.goods_title,
		c.goods_color,
		c.goods_size,
		'',
		'',
		'',
		'',
		'',
		'',
		'',
		'',
		'',
		'',
		'',
		'',
		b.goods_thumb,
		'',
		''
	FROM
		stg.zaful_eload_goods b
	JOIN
		apl_sku_color_size_mid c
	ON
		c.goods_sn = b.goods_sn
	)T
;


INSERT OVERWRITE TABLE apl_zaful_result_attr_fact
SELECT
	b.goodssn,
	b.goodsid,
	b.catid,
	b.goodstitle,
	c.goods_color,
	c.goods_size,
	b.gridurl,
	'',
	'',
	'',
	b.lang,
	'',
	'',
	'',
	'',
	'',
	'',
	b.imgurl,
	b.thumburl,
	'',
	b.urltitle
FROM
	apl_zaful_result_attr_fact a
LEFT JOIN
	apl_sku_lang_fact  b
ON
	a.goodssn = b.goodssn
LEFT JOIN
	apl_sku_color_size_fact c
ON
	a.goodssn = c.goods_sn AND c.lang = b.lang
WHERE
	b.lang != ''
;	
	
INSERT OVERWRITE TABLE  apl_zaful_result_attr_fact
SELECT
	NVL(goodssn,'')              ,
	NVL(goodsid,0)              ,
	NVL(catid,0)                ,
	NVL(goodstitle,'')           ,
	NVL(goodscolor,'')           ,
	NVL(goodssize,'')           ,
	NVL(gridurl,'')              ,
	NVL(pipelinecode,'')        ,
	NVL(shopcode,'')             ,
	NVL(webgoodSn,'')            ,
	NVL(lang,'en')                 ,
	NVL(warecode,0)             ,
	NVL(reviewcount,0)          ,
	NVL(avgrate,0)              ,
	NVL(shopprice,0)            ,
	NVL(favoritecount,0)        ,
	NVL(goodsnum,0)             ,
	NVL(imgurl,'')               ,
	NVL(thumburl,'')             ,
	NVL(thumbextendUrl,'')       ,
	NVL(urltitle,'')            
FROM
	apl_zaful_result_attr_fact;


INSERT OVERWRITE TABLE  apl_zaful_result_attr_fact
SELECT
	goodssn,
	collect_set(goodsid)[0],
	collect_set(catid)[0]                ,
	collect_set(goodstitle)[0]           ,
	collect_set(goodscolor)[0]           ,
	collect_set(goodssize)[0]           ,
	collect_set(gridurl)[0]              ,
	pipelinecode,
	collect_set(shopcode)[0]             ,
	collect_set(webgoodSn)[0]            ,
	lang ,
	collect_set(warecode)[0]             ,
	collect_set(reviewcount)[0]          ,
	collect_set(avgrate)[0]              ,
	collect_set(shopprice)[0]            ,
	collect_set(favoritecount)[0]        ,
	collect_set(goodsnum)[0]             ,
	collect_set(imgurl)[0]               ,
	collect_set(thumburl)[0]             ,
	collect_set(thumbextendUrl)[0]       ,
	collect_set(urltitle)[0]            
FROM
	apl_zaful_result_attr_fact
GROUP BY goodssn,pipelinecode,lang
;


set mapred.reduce.tasks=30;
INSERT OVERWRITE TABLE  apl_zaful_result_attr_fact_test
select * from apl_zaful_result_attr_fact 
distribute by rand(123); 

	
