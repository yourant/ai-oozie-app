--@author wuchao
--@date 2018年12月17日 
--@desc  rosegal邮件推荐国家热销数据

SET mapred.job.name=apl_rosegal_goods_country_hotsell;
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


create table if not exists dw_proj.rg_uc_map_wuc(
 user_id                        STRING    comment '用户id',
 country_code              STRING       comment '国家简码',
 country_name            STRING       comment '国家名称'
 )
 ROW FORMAT DELIMITED FIELDS TERMINATED BY '\u0001' LINES TERMINATED BY '\n' STORED AS TEXTFILE
 ;

--用户国家对应关系
INSERT OVERWRITE TABLE dw_proj.rg_uc_map_wuc SELECT
	m.user_id,
	m.country_code,
	m.country_name
FROM
	(
		SELECT
			user_id,
			country_code,
			country_name
		FROM
			dw_proj.rg_uc_map_wuc
		UNION ALL
			SELECT
				user_id,
				country_code,
				country_name
			FROM
				ods.ods_pc_burial_log
			WHERE
				concat(YEAR, MONTH, DAY) = '${DATE}'
			AND site = 'rosegal'
			AND user_id RLIKE '^[0-9]+$'
			AND country_code <> ''
			AND country_code IS NOT NULL
	) m
GROUP BY
	m.user_id,
	m.country_code,
	m.country_name;

create table if not exists dw_rg_recommend.rg_goods_country_count(
 goods_sn                        STRING    comment '商品id',
 goods_number              int       comment '商品数量',
 country_code            STRING       comment '国家简码'
 )
 ROW FORMAT DELIMITED FIELDS TERMINATED BY '\u0001' LINES TERMINATED BY '\n' STORED AS TEXTFILE
 ;

--按国家取前15天spu销量
INSERT OVERWRITE TABLE dw_rg_recommend.rg_goods_country_count 
SELECT
	b.goods_sn,
	b.goods_number,
	d.country_code
FROM
	(
		SELECT
			a.goods_sn,
			a.goods_number,
			e.user_id
		FROM
			(
				SELECT
					goods_sn,
					goods_number,
					order_id
				FROM
					ods.ods_m_rosegal_eload_order_goods
				WHERE
					dt = '${DATE}'
			) a
		JOIN (
			SELECT
				order_id,
				user_id
			FROM
				ods.ods_m_rosegal_eload_order_info
			WHERE
				dt = '${DATE}'
			AND order_status IN (1, 2, 3, 4, 6, 8, 15, 16, 20)
			AND add_time < UNIX_TIMESTAMP()
			AND add_time > UNIX_TIMESTAMP(
				DATE_SUB(
					FROM_UNIXTIME(
						UNIX_TIMESTAMP(),
						'yyyy-MM-dd'
					),
					15
				),
				'yyyy-MM-dd'
			)
		) e ON a.order_id = e.order_id
	) b
JOIN dw_proj.rg_uc_map_wuc d ON b.user_id = d.user_id;

create table if not exists dw_rg_recommend.rg_goods_country_hotsell(
 country_code            STRING       comment '国家简码',
 goods_sn                STRING       comment '商品id',
 sellcount               bigint       comment '商品数量',
 rank                    int          comment '次序'
 )
 ROW FORMAT DELIMITED FIELDS TERMINATED BY '\u0001' LINES TERMINATED BY '\n' STORED AS TEXTFILE
 ;


--热销top 50
INSERT OVERWRITE TABLE dw_rg_recommend.rg_goods_country_hotsell
SELECT
	x.country_code,
	x.goods_sn,
	x.sellcount,
	x.spurank
FROM
	(
		SELECT
			m.country_code,
			m.goods_sn,
			m.sellcount,
			ROW_NUMBER () OVER (
				PARTITION BY m.country_code
			ORDER BY
				sellcount DESC
			) AS spurank
		FROM
			(
				SELECT
					a.country_code,
					a.goods_sn,
					SUM(a.goods_number) sellcount
				FROM
					dw_rg_recommend.rg_goods_country_count a
				GROUP BY
					a.country_code,
					a.goods_sn
			) m
	) x
WHERE
	x.spurank <= 50;



CREATE TABLE IF NOT EXISTS dw_rg_recommend.apl_rg_goods_country_hotsell(
	country_code       string     COMMENT '国家',
	goodssn            string     COMMENT '推荐商品SKU',
	goodsid            string        COMMENT '商品ID',
	catid              int        COMMENT '商品分类ID',
	goodstitle         string     COMMENT '商品title',
	gridurl            string     COMMENT 'grid图url',
	lang               string     COMMENT '语言',
	imgurl             string     COMMENT '产品图url',
	thumburl           string     COMMENT '缩略图url',
	urltitle           string     COMMENT '静态页面文件标题'
	)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\u0001'                                                                                   
LINES TERMINATED BY '\n'                                                                                          
STORED AS TEXTFILE;



--关联商品池，补全商品信息
INSERT OVERWRITE TABLE dw_rg_recommend.apl_rg_goods_country_hotsell
SELECT
	NVL(a.country_code,'')              ,
	NVL(b.goods_sn,'')              ,
	NVL(b.goods_id,0)              ,
	NVL(b.cat_id,0)                ,
	NVL(b.goods_title,'')           ,
	NVL(b.goods_grid,'')              ,
	'en'                ,
	NVL(b.goods_img,'')               ,
	NVL(b.goods_thumb,'')             ,
	NVL(b.url_title,'')            
FROM(
	SELECT
		goods_sn,
		country_code
	FROM
		dw_rg_recommend.rg_goods_country_hotsell
	) a
JOIN
	ods.ods_m_rosegal_eload_goods b
ON
	a.goods_sn = b.goods_sn
;

CREATE TABLE IF NOT EXISTS dw_rg_recommend.apl_rg_goods_country_hotsell_good_link(
	country_code       string     COMMENT '国家',
	goodssn            string     COMMENT '推荐商品SKU',
	goodsid            string        COMMENT '商品ID',
	catid              int        COMMENT '商品分类ID',
	goodstitle         string     COMMENT '商品title',
	gridurl            string     COMMENT 'grid图url',
	lang               string     COMMENT '语言',
	imgurl             string     COMMENT '产品图url',
	thumburl           string     COMMENT '缩略图url',
	urltitle           string     COMMENT '静态页面文件标题'
	)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\u0001'                                                                                   
LINES TERMINATED BY '\n'                                                                                          
STORED AS TEXTFILE;



--关联商品池，补全商品信息2
INSERT OVERWRITE TABLE dw_rg_recommend.apl_rg_goods_country_hotsell_good_link
select 
    x.country_code,
	x.goodssn,
	x.goodsid,
	x.catid,
	x.goodstitle,
	x.gridurl,
	x.lang,
	x.imgurl,
	x.thumburl,
	x.urltitle
from (

	select
	a.country_code,
	a.goodssn,
	a.goodsid,
	a.catid,
	a.goodstitle,
	a.gridurl,
	a.lang,
	a.imgurl,
	a.thumburl,
	concat_ws('-',concat_ws('/',b.cat_name,a.urltitle),a.goodsid) as urltitle
	from 
	dw_rg_recommend.apl_rg_goods_country_hotsell a 
	left JOIN
	ods.ods_m_rosegal_eload_category b
	on a.catid=b.cat_id
    ) x
group by 
    x.country_code,
	x.goodssn,
	x.goodsid,
	x.catid,
	x.goodstitle,
	x.gridurl,
	x.lang,
	x.imgurl,
	x.thumburl,
	x.urltitle
;


--全量商品
CREATE TABLE IF NOT EXISTS dw_rg_recommend.apl_rg_goods_sn_2hbase(
	goodssn            string     COMMENT '推荐商品SKU',
	goodsid            String        COMMENT '商品ID',
	catid              int        COMMENT '商品分类ID',
	goodstitle         string     COMMENT '商品title',
	gridurl            string     COMMENT 'grid图url',
	lang               string     COMMENT '语言',
	imgurl             string     COMMENT '产品图url',
	thumburl           string     COMMENT '缩略图url',
	urltitle           string     COMMENT '静态页面文件标题'
	)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\u0001'                                                                                   
LINES TERMINATED BY '\n'                                                                                          
STORED AS TEXTFILE;


INSERT OVERWRITE TABLE dw_rg_recommend.apl_rg_goods_sn_2hbase
select goods_sn,goods_id,cat_id,goods_title,goods_grid,'en',goods_img,goods_thumb,url_title
from ods.ods_m_rosegal_eload_goods
where dt='${DATE}'
group by goods_sn,goods_id,cat_id,goods_title,goods_grid,goods_img,goods_thumb,url_title
;

CREATE TABLE IF NOT EXISTS dw_rg_recommend.apl_rg_goods_sn_2hbase_good_link(
	goodssn            string     COMMENT '推荐商品SKU',
	goodsid            int        COMMENT '商品ID',
	catid              int        COMMENT '商品分类ID',
	goodstitle         string     COMMENT '商品title',
	gridurl            string     COMMENT 'grid图url',
	lang               string     COMMENT '语言',
	imgurl             string     COMMENT '产品图url',
	thumburl           string     COMMENT '缩略图url',
	urltitle           string     COMMENT '静态页面文件标题'
	)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\u0001'                                                                                   
LINES TERMINATED BY '\n'                                                                                          
STORED AS TEXTFILE;

INSERT OVERWRITE TABLE dw_rg_recommend.apl_rg_goods_sn_2hbase_good_link
select
x.goodssn,
x.goodsid,
x.catid,
x.goodstitle,
x.gridurl,
x.lang,
x.imgurl,
x.thumburl,
x.urltitle
from (

	select
	a.goodssn,
	a.goodsid,
	a.catid,
	a.goodstitle,
	a.gridurl,
	a.lang,
	a.imgurl,
	a.thumburl,
	concat_ws('-',concat_ws('/',b.cat_name,a.urltitle),a.goodsid) as urltitle

	from 
	dw_rg_recommend.apl_rg_goods_sn_2hbase a 
	left JOIN
	ods.ods_m_rosegal_eload_category b
	on a.catid=b.cat_id
    ) x
group by 
	x.goodssn,
	x.goodsid,
	x.catid,
	x.goodstitle,
	x.gridurl,
	x.lang,
	x.imgurl,
	x.thumburl,
	x.urltitle
;