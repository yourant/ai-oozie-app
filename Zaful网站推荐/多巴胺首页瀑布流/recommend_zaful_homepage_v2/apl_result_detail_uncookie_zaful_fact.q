--@author LIUQINGFAN
--@date 2018年6月28日 
--@desc  算法结果

set mapred.job.queue.name=root.ai.offline; 
SET mapred.max.split.size=128000000;
SET mapred.min.split.size=128000000;
SET mapred.min.split.size.per.node=128000000;
SET mapred.min.split.size.per.rack=128000000;
SET hive.exec.reducers.bytes.per.reducer = 128000000;
SET hive.merge.mapfiles=true;
SET hive.merge.mapredfiles= true;
SET hive.input.format=org.apache.hadoop.hive.ql.io.CombineHiveInputFormat;
SET hive.merge.size.per.task=256000000;
SET hive.exec.parallel = true; 

SET mapred.job.name='apl_result_detail_uncookie_zaful_fact';

set hive.support.concurrency=false;
set hive.auto.convert.join=false;
USE dw_zaful_recommend;

CREATE TABLE IF NOT EXISTS result_detail_uncookie_user
(
	region_id        string  COMMENT '地域ID',
	platform         string  COMMENT '平台',
	goodsn           string  COMMENT '商品SKU',
	score            int     COMMENT '商品评分'
	)
COMMENT "新用户推荐商品"
PARTITIONED BY(year string,month string,day string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\u0001' 
LINES TERMINATED BY '\n' 
STORED AS TEXTFILE;

CREATE TABLE IF NOT EXISTS apl_result_detail_uncookie_zaful_fact(
	regionid            string     COMMENT '地域ID',
	platform            string  COMMENT '平台',
	goodssn            string     COMMENT '推荐商品SKU',
	goodsid            int        COMMENT '商品ID',
	goodssn2		   string     COMMENT '推荐商品SKU',
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
	urltitle           string     COMMENT '静态页面文件标题',
	score              int        COMMENT '排序字段'
	)
COMMENT 'COOKIE用户推荐结果'
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\u0001'                                                                                   
LINES TERMINATED BY '\n'                                                                                          
STORED AS TEXTFILE;


INSERT OVERWRITE TABLE apl_result_detail_uncookie_zaful_fact
SELECT
	region_id,
	platform,
	goods_sn,
	goods_id,
	goods_sn,
	cat_id,
	goods_title,
	'',
	'',
	goods_grid,
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
	'',
	'',
	score
FROM(
	SELECT
		a.region_id,
		a.platform,
		b.goods_sn,
		b.goods_id,
		b.cat_id,
		b.goods_title,
		b.goods_grid,
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
		'',
		'',
		a.score
	FROM(
		SELECT
			region_id,
			platform,
			goodsn,
			score
		FROM
			result_detail_uncookie_user
		WHERE
			concat(year,month,day) = ${ADD_TIME}
		)a
	JOIN
		stg.zaful_eload_goods b
	ON
		a.goodsn = b.goods_sn
	WHERE
		b.is_on_sale = 1  AND b.is_delete = 0  AND b.goods_number > 0 
	)T
;
INSERT OVERWRITE TABLE apl_result_detail_uncookie_zaful_fact
SELECT
	a.regionid,
	a.platform,
	a.goodssn,
	b.goodsid,
	a.goodssn,
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
	apl_result_detail_uncookie_zaful_fact a
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



	
INSERT OVERWRITE TABLE  apl_result_detail_uncookie_zaful_fact
SELECT
	NVL(regionid,'')              ,
	NVL(platform,'')              ,
	NVL(goodssn,'')              ,
	NVL(goodsid,0)              ,
	NVL(goodssn2,''),
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
	NVL(urltitle,'')   ,
	NVL(score,0)  score
FROM
	apl_result_detail_uncookie_zaful_fact
ORDER BY 
	score;
	

	

