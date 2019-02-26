--@author LIUQINGFAN
--@date 2018年5月30日 
--@desc  商品购物车页有COOKIE推荐位结果输出

SET mapred.job.name='apl_result_detail_cookie_zaful_fact';

set mapred.job.queue.name=root.ai.online; 

USE dw_zaful_recommend;

CREATE TABLE IF NOT EXISTS result_detail_cookie_user
(
	cookie          string  COMMENT '用户COOKIE',
	goodsn         string  COMMENT '推荐商品',
	score            int     COMMENT '商品评分'
	)
COMMENT "购物车页有cookie推荐结果"
PARTITIONED BY(year string,month string,day string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\u0001' 
LINES TERMINATED BY '\n' 
STORED AS TEXTFILE;



CREATE TABLE IF NOT EXISTS apl_result_detail_cookie_zaful_fact(
	cookie             string     COMMENT '用户COOKIE',
	goodssn           string     COMMENT '推荐商品SKU',
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
COMMENT '购物车页有cookie推荐结果'
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\u0001'                                                                                   
LINES TERMINATED BY '\n'                                                                                          
STORED AS TEXTFILE;







INSERT OVERWRITE TABLE apl_result_detail_cookie_zaful_fact
SELECT
	cookie,
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
	'',
	score
FROM(
	SELECT
		a.cookie,
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
		a.score
	FROM(
		SELECT
			cookie,
			goodsn,
			score
		FROM
			result_detail_cookie_user
		WHERE
			concat(year,month,day) = ${ADD_TIME}
		)a
	JOIN
		stg.zaful_eload_goods b
	ON
		a.goodsn = b.goods_sn
	JOIN
		apl_sku_color_size_mid c
	ON
		c.goods_sn = a.goodsn
	)T
;


INSERT OVERWRITE TABLE apl_result_detail_cookie_zaful_fact
SELECT
	a.cookie,
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
	apl_result_detail_cookie_zaful_fact a
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
	
INSERT OVERWRITE TABLE  apl_result_detail_cookie_zaful_fact
SELECT
	NVL(cookie,'')              ,
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
	NVL(urltitle,'')            ,
	NVL(score,0) score
FROM
	apl_result_detail_cookie_zaful_fact
ORDER BY
	score;
	
	
	