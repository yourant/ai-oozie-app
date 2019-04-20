--@author ZhanRui
--@date 2019年01月10日 
--@desc  zaful ods基础数据整合

SET mapred.job.name=zaful_ods_base;
set mapred.job.queue.name=root.ai.offline;
SET mapred.max.split.size=128000000;
SET mapred.min.split.size=64000000;
SET mapred.min.split.size.per.node=64000000;
SET mapred.min.split.size.per.rack=64000000;
SET hive.exec.reducers.bytes.per.reducer = 64000000;
SET hive.merge.mapfiles=true;
SET hive.merge.mapredfiles= true;
SET hive.input.format=org.apache.hadoop.hive.ql.io.CombineHiveInputFormat;
SET hive.merge.size.per.task=256000000;
SET hive.exec.parallel = true;
set hive.support.concurrency=false;


CREATE TABLE IF NOT EXISTS tmp.apl_sku_lang_mid(
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



--SKU多语言中间表
INSERT OVERWRITE TABLE tmp.apl_sku_lang_mid
SELECT
	 goods_id,
	 goods_title,
	 is_lang_show,
	 url_title,
	 'ar' lang
FROM
	ods.ods_m_zaful_eload_goods_ar
    WHERE dt='${DATE}'
UNION  ALL
SELECT
	 goods_id,
	 goods_title,
	 is_lang_show,
	 url_title,
	 'es' lang
FROM
	ods.ods_m_zaful_eload_goods_es
    WHERE dt='${DATE}'
UNION  ALL
SELECT
	 goods_id,
	 goods_title,
	 is_lang_show,
	 url_title,
	 'de' lang
FROM
	ods.ods_m_zaful_eload_goods_de
    WHERE dt='${DATE}'
UNION  ALL
SELECT
	 goods_id,
	 goods_title,
	 is_lang_show,
	 url_title,
	 'fr' lang
FROM
	ods.ods_m_zaful_eload_goods_fr 
    WHERE dt='${DATE}'
UNION  ALL
SELECT
	 goods_id,
	 goods_title,
	 is_lang_show,
	 url_title,
	 'pt' lang
FROM
	ods.ods_m_zaful_eload_goods_pt
    WHERE dt='${DATE}'
UNION  ALL
SELECT
	 goods_id,

	 goods_title,
	 is_lang_show,
	 url_title,
	 'it' lang
FROM
	ods.ods_m_zaful_eload_goods_it
    WHERE dt='${DATE}'
UNION ALL
SELECT
	 goods_id,
	 goods_title,
	 is_lang_show,
	 url_title,
	 'en' lang
FROM
	ods.ods_m_zaful_eload_goods
    WHERE dt='${DATE}'
UNION ALL
SELECT
	 goods_id,
	 goods_title,
	 is_lang_show,
	 url_title,
	 'id' lang
FROM
	ods.ods_m_zaful_eload_goods_id
    WHERE dt='${DATE}'
UNION ALL
SELECT
	 goods_id,
	 goods_title,
	 is_lang_show,
	 url_title,
	 'th' lang
FROM
	ods.ods_m_zaful_eload_goods_th
    WHERE dt='${DATE}'
UNION ALL
SELECT
	 goods_id,
	 goods_title,
	 is_lang_show,
	 url_title,
	 'zh-tw' lang
FROM
	ods.ods_m_zaful_eload_goods_zhtw
    WHERE dt='${DATE}'
UNION ALL
SELECT
	 goods_id,
	 goods_title,
	 is_lang_show,
	 url_title,
	 'tr' lang
FROM
	ods.ods_m_zaful_eload_goods_tr 
    WHERE dt='${DATE}'
UNION ALL
SELECT
	 goods_id,
	 goods_title,
	 is_lang_show,
	 url_title,
	 'ru' lang
FROM
	ods.ods_m_zaful_eload_goods_ru 
    WHERE dt='${DATE}'
;



CREATE TABLE IF NOT EXISTS tmp.apl_sku_pipling_lang(
	goods_id           int       COMMENT '商品ID',
	shop_price         float    COMMENT '商品售价',
    pipelinecode       string     COMMENT '渠道编码',
	lang               string    COMMENT '语言'
	)
COMMENT "SKU渠道语言中间表"
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\u0001' 
LINES TERMINATED BY '\n' 
STORED AS TEXTFILE;	

--SKU渠道语言中间表
INSERT OVERWRITE TABLE tmp.apl_sku_pipling_lang
SELECT
	goods_id,
	shop_price,
	'ZF' AS pipelinecode,
	'en' AS lang
FROM
	ods.ods_m_zaful_eload_goods
WHERE
	dt = '${DATE}'
AND is_lang_show = 1
UNION  ALL
SELECT
	goods_id,
	shop_price,
	'ZFFR' AS pipelinecode,
	'fr' AS lang
FROM
	ods.ods_m_zaful_eload_goods_pipeline_zffr
WHERE
	dt = '${DATE}'
AND is_show = 1
UNION  ALL
SELECT
	goods_id,
	shop_price,
	'ZFDE' AS pipelinecode,
	'de' AS lang
FROM
	ods.ods_m_zaful_eload_goods_pipeline_zfde
WHERE
	dt = '${DATE}'
AND is_show = 1
UNION  ALL
SELECT
	goods_id,
	shop_price,
	'ZFES' AS pipelinecode,
	'es' AS lang
FROM
	ods.ods_m_zaful_eload_goods_pipeline_zfes
WHERE
	dt = '${DATE}'
AND is_show = 1
UNION  ALL
SELECT
	goods_id,
	shop_price,
	'ZFIT' AS pipelinecode,
	'it' AS lang
FROM
	ods.ods_m_zaful_eload_goods_pipeline_zfit
WHERE
	dt = '${DATE}'
AND is_show = 1
UNION  ALL
SELECT
	goods_id,
	shop_price,
	'ZFPT' AS pipelinecode,
	'pt' AS lang
FROM
	ods.ods_m_zaful_eload_goods_pipeline_zfpt
WHERE
	dt = '${DATE}'
AND is_show = 1
UNION  ALL
SELECT
	goods_id,
	shop_price,
	'ZFAR' AS pipelinecode,
	'ar' AS lang
FROM
	ods.ods_m_zaful_eload_goods_pipeline_zfar
WHERE
	dt = '${DATE}'
AND is_show = 1
UNION  ALL
SELECT
	goods_id,
	shop_price,
	'ZFCA' AS pipelinecode,
	'en' AS lang
FROM
	ods.ods_m_zaful_eload_goods_pipeline_zfca
WHERE
	dt = '${DATE}'
AND is_show = 1
UNION  ALL
SELECT
	goods_id,
	shop_price,
	'ZFGB' AS pipelinecode,
	'en' AS lang
FROM
	ods.ods_m_zaful_eload_goods_pipeline_zfgb
WHERE
	dt = '${DATE}'
AND is_show = 1
UNION  ALL
SELECT
	goods_id,
	shop_price,
	'ZFIE' AS pipelinecode,
	'en' AS lang
FROM
	ods.ods_m_zaful_eload_goods_pipeline_zfie
WHERE
	dt = '${DATE}'
AND is_show = 1
UNION  ALL
SELECT
	goods_id,
	shop_price,
	'ZFAU' AS pipelinecode,
	'en' AS lang
FROM
	ods.ods_m_zaful_eload_goods_pipeline_zfau
WHERE
	dt = '${DATE}'
AND is_show = 1
UNION  ALL
SELECT
	goods_id,
	shop_price,
	'ZFNZ' AS pipelinecode,
	'en' AS lang
FROM
	ods.ods_m_zaful_eload_goods_pipeline_zfnz
WHERE
	dt = '${DATE}'
AND is_show = 1
UNION  ALL
SELECT
	goods_id,
	shop_price,
	'ZFCH' AS pipelinecode,
	'de' AS lang
FROM
	ods.ods_m_zaful_eload_goods_pipeline_zfch
WHERE
	dt = '${DATE}'
AND is_show = 1
UNION  ALL
SELECT
	goods_id,
	shop_price,
	'ZFBE' AS pipelinecode,
	'fr' AS lang
FROM
	ods.ods_m_zaful_eload_goods_pipeline_zfbe
WHERE
	dt = '${DATE}'
AND is_show = 1
UNION  ALL
SELECT
	goods_id,
	shop_price,
	'ZFPH' AS pipelinecode,
	'en' AS lang
FROM
	ods.ods_m_zaful_eload_goods_pipeline_zfph
WHERE
	dt = '${DATE}'
AND is_show = 1
UNION  ALL
SELECT
	goods_id,
	shop_price,
	'ZFSG' AS pipelinecode,
	'en' AS lang
FROM
	ods.ods_m_zaful_eload_goods_pipeline_zfsg
WHERE
	dt = '${DATE}'
AND is_show = 1
UNION  ALL
SELECT
	goods_id,
	shop_price,
	'ZFMY' AS pipelinecode,
	'en' AS lang
FROM
	ods.ods_m_zaful_eload_goods_pipeline_zfmy
WHERE
	dt = '${DATE}'
AND is_show = 1
UNION  ALL
SELECT
	goods_id,
	shop_price,
	'ZFIN' AS pipelinecode,
	'en' AS lang
FROM
	ods.ods_m_zaful_eload_goods_pipeline_zfin
WHERE
	dt = '${DATE}'
AND is_show = 1
UNION  ALL
SELECT
	goods_id,
	shop_price,
	'ZFID' AS pipelinecode,
	'id' AS lang
FROM
	ods.ods_m_zaful_eload_goods_pipeline_zfid
WHERE
	dt = '${DATE}'
AND is_show = 1
UNION  ALL
SELECT
	goods_id,
	shop_price,
	'ZFBR' AS pipelinecode,
	'pt' AS lang
FROM
	ods.ods_m_zaful_eload_goods_pipeline_zfbr
WHERE
	dt = '${DATE}'
AND is_show = 1
UNION  ALL
SELECT
	goods_id,
	shop_price,
	'ZFMX' AS pipelinecode,
	'es' AS lang
FROM
	ods.ods_m_zaful_eload_goods_pipeline_zfmx
WHERE
	dt = '${DATE}'
AND is_show = 1
UNION  ALL
SELECT
	goods_id,
	shop_price,
	'ZFTH' AS pipelinecode,
	'th' AS lang
FROM
	ods.ods_m_zaful_eload_goods_pipeline_zfth
WHERE
	dt = '${DATE}'
AND is_show = 1
UNION  ALL
SELECT
	goods_id,
	shop_price,
	'ZFTW' AS pipelinecode,
	'zh-tw' AS lang
FROM
	ods.ods_m_zaful_eload_goods_pipeline_zftw
WHERE
	dt = '${DATE}'
AND is_show = 1
UNION  ALL
SELECT
	goods_id,
	shop_price,
	'ZFRU' AS pipelinecode,
	'ru' AS lang
FROM
	ods.ods_m_zaful_eload_goods_pipeline_zfru
WHERE
	dt = '${DATE}'
AND is_show = 1
UNION  ALL
SELECT
	goods_id,
	shop_price,
	'ZFTR' AS pipelinecode,
	'tr' AS lang
FROM
	ods.ods_m_zaful_eload_goods_pipeline_zftr
WHERE
	dt = '${DATE}'
AND is_show = 1
UNION  ALL
SELECT
	goods_id,
	shop_price,
	'ZFZA' AS pipelinecode,
	'en' AS lang
FROM
	ods.ods_m_zaful_eload_goods_pipeline_zfza
WHERE
	dt = '${DATE}'
AND is_show = 1
UNION  ALL
SELECT
	goods_id,
	shop_price,
	'ZFAT' AS pipelinecode,
	'de' AS lang
FROM
	ods.ods_m_zaful_eload_goods_pipeline_zfat
WHERE
	dt = '${DATE}'
AND is_show = 1
UNION  ALL
SELECT
	goods_id,
	shop_price,
	'ZFIL' AS pipelinecode,
	'en' AS lang
FROM
	ods.ods_m_zaful_eload_goods_pipeline_zfil
WHERE
	dt = '${DATE}'
AND is_show = 1
;


CREATE TABLE IF NOT EXISTS tmp.apl_sku_color_size_fact(
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


--多语言商品颜色、尺寸属性中间表
INSERT OVERWRITE TABLE tmp.apl_sku_color_size_fact SELECT
	g.goods_id,
	g.goods_sn,

IF (
	gam_color.attr_value_lang != '',
	gam_color.attr_value_lang,
	ga_color.attr_value
) goods_color,

IF (
	gam_size.attr_value_lang != '',
	gam_size.attr_value_lang,
	ga_size.attr_value
) goods_size,
 gam_color.lang
FROM
	(
		SELECT
			*
		FROM
			ods.ods_m_zaful_eload_goods
		WHERE
			dt = '${DATE}'
	) g
LEFT JOIN (
	SELECT
		*
	FROM
		ods.ods_m_zaful_eload_goods_attr
	WHERE
		dt = '${DATE}'
) ga_size ON g.goods_id = ga_size.goods_id
AND ga_size.`attr_id` = 7
LEFT JOIN (
	SELECT
		*
	FROM
		ods.ods_m_zaful_eload_goods_attr
	WHERE
		dt = '${DATE}'
) ga_color ON g.goods_id = ga_color.goods_id
AND ga_color.`attr_id` = 8
LEFT JOIN (
	SELECT
		*
	FROM
		ods.ods_m_zaful_eload_goods_attr_multi_lang
	WHERE
		dt = '${DATE}'
) gam_color ON ga_color.attr_id = gam_color.attr_id
AND ga_color.attr_value = gam_color.attr_value
AND gam_color.`attr_id` = 8
LEFT JOIN (
	SELECT
		*
	FROM
		ods.ods_m_zaful_eload_goods_attr_multi_lang
	WHERE
		dt = '${DATE}'
) gam_size ON ga_size.attr_id = gam_size.attr_id
AND ga_size.attr_value = gam_size.attr_value
AND gam_size.`attr_id` = 7
WHERE
	g.is_on_sale = 1
AND g.is_delete = 0
AND g.goods_number > 0
AND gam_color.lang != ''
UNION ALL
	SELECT
		g.goods_id,
		g.goods_sn,
		ga_color.attr_value goods_color,
		ga_size.attr_value goods_size,
		'en' lang
	FROM
		(
			SELECT
				*
			FROM
				ods.ods_m_zaful_eload_goods
			WHERE
				dt = '${DATE}'
		) g
	LEFT JOIN (
		SELECT
			*
		FROM
			ods.ods_m_zaful_eload_goods_attr
		WHERE
			dt = '${DATE}'
	) ga_size ON g.goods_id = ga_size.goods_id
	AND ga_size.`attr_id` = 7
	LEFT JOIN (
		SELECT
			*
		FROM
			ods.ods_m_zaful_eload_goods_attr
		WHERE
			dt = '${DATE}'
	) ga_color ON g.goods_id = ga_color.goods_id
	AND ga_color.`attr_id` = 8
	WHERE
		g.is_on_sale = 1
	AND g.is_delete = 0
	AND g.goods_number > 0;


INSERT OVERWRITE TABLE tmp.apl_sku_color_size_fact
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
		tmp.apl_sku_color_size_fact
     
	) t
WHERE
	flag = 1;




CREATE TABLE IF NOT EXISTS tmp.apl_zaful_result_attr_fact(
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


--商品属性结果表
INSERT OVERWRITE TABLE tmp.apl_zaful_result_attr_fact SELECT
	m.goodssn,
	m.goodsid,
	m.catid,
	m.goodstitle,
	n.goods_color,
	n.goods_size,
	m.gridurl,
	m.pipelinecode,
	m.shopcode,
	m.webgoodSn,
	m.lang,
	m.warecode,
	m.reviewcount,
	m.avgrate,
	m.shopprice,
	m.favoritecount,
	m.goodsnum,
	m.imgurl,
	m.thumburl,
	m.thumbextendUrl,
	m.urltitle
FROM
	(
		SELECT
			b.goods_sn AS goodssn,
			b.goods_id AS goodsid,
			b.cat_id AS catid,
			a.goods_title AS goodstitle,
			b.goods_grid AS gridurl,
			c.pipelinecode AS pipelinecode,
			'' AS shopcode,
			'' AS webgoodSn,
			a.lang,
			'' AS warecode,
			'' AS reviewcount,
			'' AS avgrate,
			c.shop_price AS shopprice,
			'' AS favoritecount,
			b.goods_number AS goodsnum,
			b.goods_img AS imgurl,
			b.goods_thumb AS thumburl,
			'' AS thumbextendUrl,
			a.url_title AS urltitle
		FROM
			tmp.apl_sku_lang_mid a
		JOIN (
			SELECT
				*
			FROM
				ods.ods_m_zaful_eload_goods
			WHERE
				dt = '${DATE}' AND is_on_sale = 1
		) b ON a.goods_id = b.goods_id
        JOIN tmp.apl_sku_pipling_lang c 
        ON  a.goods_id = c.goods_id and a.lang = c.lang 
	) m
LEFT JOIN tmp.apl_sku_color_size_fact n ON m.goodssn = n.goods_sn
AND m.lang = n.lang;



CREATE TABLE IF NOT EXISTS tmp.apl_nodetree_zf_fact( 
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


--商品等级分类表(基础数据供使用)
INSERT OVERWRITE TABLE  tmp.apl_nodetree_zf_fact
SELECT 
	cat_id, 
	cat_name,
	split(d.NODE,',')[0] node1, --一级分类
	split(d.NODE,',')[1] node2, --二级分类
	split(d.NODE,',')[2] node3, --三级分类
	split(d.NODE,',')[3] node4  --四级分类
FROM 
    ods.ods_m_zaful_eload_category d
WHERE d.dt='${DATE}'
;


CREATE TABLE IF NOT EXISTS tmp.apl_sku_addtime_zf_fact( 
	cat_id         string        COMMENT 'sku分类cat_id',
	goods_sn       string        COMMENT '商品SKU',
	add_time       string        COMMENT '商品上架时间'
)
COMMENT '商品等级分类表'
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\u0001'                                                                                   
LINES TERMINATED BY '\n'                                                                                          
STORED AS TEXTFILE;


--上架时间排序 计算新品用
INSERT OVERWRITE TABLE tmp.apl_sku_addtime_zf_fact
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
		ods.ods_m_zaful_eload_goods		
	WHERE
		dt = '${DATE}' AND is_on_sale = 1  AND is_delete = 0  AND goods_number > 0
	)tmp
WHERE
	flag < 61;


CREATE TABLE IF NOT EXISTS tmp.apl_result_newgoods_zaful_fact(
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

--新品推荐结果
INSERT OVERWRITE TABLE tmp.apl_result_newgoods_zaful_fact
SELECT
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
	NVL(b.lang,'')                 ,
	NVL(b.warecode,0)             ,
	NVL(b.reviewcount,0)          ,
	NVL(b.avgrate,0)              ,
	NVL(b.shopprice,0)            ,
	NVL(b.favoritecount,0)        ,
	NVL(b.goodsnum,0)             ,
	NVL(b.imgurl,'')               ,
	NVL(b.thumburl,'')             ,
	NVL(b.thumbextendUrl,'')       ,
	NVL(b.urltitle,'')             
	FROM
		tmp.apl_sku_addtime_zf_fact a
	JOIN
		tmp.apl_zaful_result_attr_fact b
	ON
		a.goods_sn = b.goodssn;



CREATE TABLE IF NOT EXISTS tmp.apl_hotsell_zf_mid( 
	cat_id             string        COMMENT 'sku分类cat_id',
	goods_sn           string        COMMENT '商品SKU',
	goods_number       int           COMMENT '商品销量'
)
COMMENT '商品分类销量中间表'
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\u0001'                                                                                   
LINES TERMINATED BY '\n'                                                                                          
STORED AS TEXTFILE;


--销量排序 计算热销用
INSERT OVERWRITE TABLE tmp.apl_hotsell_zf_mid
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
				ods.ods_m_zaful_eload_order_goods
			WHERE
                dt='${DATE}' and 
				from_unixtime(addtime,'yyyyMMdd') > '${WDATE}'
			GROUP BY
				goods_sn
			)a
		JOIN
			(SELECT * from ods.ods_m_zaful_eload_goods WHERE dt='${DATE}')  b
		ON
			a.goods_sn = b.goods_sn
		)tmp
	)tmp
WHERE
	flag < 61;


	
CREATE TABLE IF NOT EXISTS tmp.apl_result_hotsell_cat_zaful_fact(
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


--分类热销推荐结果
INSERT OVERWRITE TABLE tmp.apl_result_hotsell_cat_zaful_fact
SELECT
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
	NVL(b.lang,'')                 ,
	NVL(b.warecode,0)             ,
	NVL(b.reviewcount,0)          ,
	NVL(b.avgrate,0)              ,
	NVL(b.shopprice,0)            ,
	NVL(b.favoritecount,0)        ,
	NVL(b.goodsnum,0)             ,
	NVL(b.imgurl,'')               ,
	NVL(b.thumburl,'')             ,
	NVL(b.thumbextendUrl,'')       ,
	NVL(b.urltitle,'')             
	FROM
		tmp.apl_hotsell_zf_mid a
	JOIN
		tmp.apl_zaful_result_attr_fact b
	ON
		a.goods_sn = b.goodssn;


CREATE TABLE IF NOT EXISTS tmp.apl_result_hotsell_all_zaful_fact(
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



--网站热销推荐结果
INSERT OVERWRITE TABLE tmp.apl_result_hotsell_all_zaful_fact
SELECT
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
	NVL(b.lang,'')                 ,
	NVL(b.warecode,0)             ,
	NVL(b.reviewcount,0)          ,
	NVL(b.avgrate,0)              ,
	NVL(b.shopprice,0)            ,
	NVL(b.favoritecount,0)        ,
	NVL(b.goodsnum,0)             ,
	NVL(b.imgurl,'')               ,
	NVL(b.thumburl,'')             ,
	NVL(b.thumbextendUrl,'')       ,
	NVL(b.urltitle,'')             
	FROM
		(SELECT
			goods_sn,
			SUM(goods_number) goods_number
		FROM
			ods.ods_m_zaful_eload_order_goods
		WHERE
            dt='${DATE}' and 
			from_unixtime(addtime,'yyyyMMdd') > '${WDATE}'
		GROUP BY
			goods_sn
		ORDER BY
			goods_number DESC
		LIMIT 60) a
	JOIN
		tmp.apl_zaful_result_attr_fact b
	ON
		a.goods_sn = b.goodssn;