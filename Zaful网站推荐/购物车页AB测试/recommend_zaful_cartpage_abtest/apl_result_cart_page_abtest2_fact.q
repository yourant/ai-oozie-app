--@author LIUQINGFAN
--@date 2018年5月30日 
--@desc  商品购物车页推荐位结果输出

SET mapred.job.name='apl_result_cart_page_abtest_fact2';
set mapred.job.queue.name=root.ai.offline; 
set hive.support.concurrency=false;
SET hive.auto.convert.join=false;
USE dw_zaful_recommend;

CREATE TABLE IF NOT EXISTS result_cart_page_abtest2
(
	goods_spu1        string  COMMENT '商品SPU',
	goods_sn2         string  COMMENT '推荐商品',
	score            int     COMMENT '商品评分'
	)
COMMENT "购物车页ABTEST推荐商品"
PARTITIONED BY(year string,month string,day string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\u0001' 
LINES TERMINATED BY '\n' 
STORED AS TEXTFILE;


CREATE TABLE IF NOT EXISTS apl_result_cart_page_abtest_fact2(
	goodsspu1           string     COMMENT '商详页展示商品',
	goodssn2           string     COMMENT '推荐商品SKU',
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
	urltitle           string     COMMENT '静态页面文件标题',
	score              int        COMMENT '排序字段'
	)
COMMENT '购物车页ABTEST推荐结果'
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\u0001'                                                                                   
LINES TERMINATED BY '\n'                                                                                          
STORED AS TEXTFILE;





INSERT OVERWRITE TABLE apl_result_cart_page_abtest_fact2
SELECT
	goodsspu1,
	goods_sn2,
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
	'',
	score
FROM(
	SELECT
		a.goods_spu1 goodsspu1,
		b.goods_sn goods_sn2,
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
		a.score
	FROM(
		SELECT
			goods_spu1,
			goods_sn2,
			score
		FROM
			result_cart_page_abtest2
		WHERE
			concat(year,month,day) = ${DATE}
		)a
	JOIN
		stg.zaful_eload_goods b
	ON
		a.goods_sn2 = b.goods_sn
	JOIN
		apl_sku_color_size_mid c
	ON
		c.goods_sn = a.goods_sn2
	)T
;


INSERT OVERWRITE TABLE apl_result_cart_page_abtest_fact2
SELECT
	a.goodsspu1,
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
	b.urltitle,
	a.score
FROM
	apl_result_cart_page_abtest_fact2 a
LEFT JOIN
	apl_sku_lang_fact  b
ON
	a.goodssn2 = b.goodssn
LEFT JOIN
	apl_sku_color_size_fact c
ON
	a.goodssn2 = c.goods_sn AND c.lang = b.lang
WHERE
	b.lang != ''
;	
	
INSERT OVERWRITE TABLE  apl_result_cart_page_abtest_fact2
SELECT
	NVL(goodsspu1,'')              ,
	NVL(goodssn2,'')              ,
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
	NVL(urltitle,'')            ,
	NVL(score,0) score
FROM
	apl_result_cart_page_abtest_fact2
ORDER BY
	score;
	
	
	