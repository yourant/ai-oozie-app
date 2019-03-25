--@author ZhanRui
--@date 2019年03月25日 
--@desc  dresslily邮件推荐国家热销数据

SET mapred.job.name=apl_dl_goods_country_hotsell;
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


--商品信息
CREATE TABLE IF NOT EXISTS dw_dresslily_recommend.apl_dresslily_goods_info(
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


INSERT OVERWRITE TABLE dw_dresslily_recommend.apl_dresslily_goods_info SELECT
	goods_sn,
	goods_id,
	cat_id,
	goods_title,
	goods_grid,
	'en',
	goods_img,
	goods_thumb,
	url_title
FROM
	ods.ods_m_dresslily_eload_goods
WHERE
	dt = '${DATE}'
GROUP BY
	goods_sn,
	goods_id,
	cat_id,
	goods_title,
	goods_grid,
	goods_img,
	goods_thumb,
	url_title;


--国家热销
INSERT OVERWRITE TABLE dw_proj.dl_uc_map SELECT
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
			dw_proj.dl_uc_map
		UNION ALL
			SELECT
				user_id,
				country_code,
				country_name
			FROM
				ods.ods_pc_burial_log
			WHERE
				concat(YEAR, MONTH, DAY) = '${DATE}'
			AND site = 'dresslily'
			AND user_id RLIKE '^[0-9]+$'
			AND country_code <> ''
			AND country_code IS NOT NULL
	) m
GROUP BY
	m.user_id,
	m.country_code,
	m.country_name;


--按国家取前15天spu销量
INSERT OVERWRITE TABLE dw_dresslily_recommend.goods_country_count SELECT
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
					ods.ods_m_dresslily_eload_order_goods
				WHERE
					dt = '${DATE}'
			) a
		JOIN (
			SELECT
				order_id,
				user_id
			FROM
				ods.ods_m_dresslily_eload_order_info
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
JOIN dw_proj.dl_uc_map d ON b.user_id = d.user_id;




--热销top 30
INSERT OVERWRITE TABLE dw_dresslily_recommend.goods_country_hotsell
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
					dw_dresslily_recommend.goods_country_count a
				GROUP BY
					a.country_code,
					a.goods_sn
			) m
	) x
WHERE
	x.spurank <= 30;


CREATE TABLE IF NOT EXISTS dw_dresslily_recommend.apl_dl_goods_country_hotsell(
	country_code       string     COMMENT '国家',
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


--关联商品池，补全商品信息
INSERT OVERWRITE TABLE dw_dresslily_recommend.apl_dl_goods_country_hotsell
SELECT
	NVL(a.country_code,'')              ,
	NVL(b.goodssn,'')              ,
	NVL(b.goodsid,0)              ,
	NVL(b.catid,0)                ,
	NVL(b.goodstitle,'')           ,
	NVL(b.gridurl,'')              ,
	NVL(b.lang,'en')                 ,
	NVL(b.imgurl,'')               ,
	NVL(b.thumburl,'')             ,
	NVL(b.urltitle,'')            
FROM(
	SELECT
		goods_sn,
		country_code
	FROM
		dw_dresslily_recommend.goods_country_hotsell
	) a
JOIN
	dw_dresslily_recommend.apl_dresslily_goods_info b
ON
	a.goods_sn = b.goodssn
;
