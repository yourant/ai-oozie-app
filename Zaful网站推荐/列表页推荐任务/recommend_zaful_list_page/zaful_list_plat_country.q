--@author 
--@date 2018年6月25日 
--@desc  国家点击量超过1000数据

SET mapred.job.name='';
set mapred.job.queue.name=root.ai.online; 
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

USE dw_zaful_recommend;


CREATE TABLE IF NOT EXISTS goods_date_exp_cat_country_temp(
	date        string   COMMENT '日期',
	glb_plf     string   COMMENT '平台',
	goods_sn    string   COMMENT '商品SKU',
	cat_id      string   COMMENT '分类',
	country     string   COMMENT '国家',
	num         int      COMMENT '商品曝光数量'
	)
COMMENT "SKU曝光数据统计"
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\u0001' 
LINES TERMINATED BY '\n' 
STORED AS TEXTFILE;
--曝光
INSERT OVERWRITE TABLE goods_date_exp_cat_country_temp
SELECT
	date,
	glb_plf,
	goods_sn,
	cat_id,
	country,
	count(*) num
FROM(
	SELECT
		a.date,
		b.glb_plf,
		b.cat_id,
		b.country,
		get_json_object(a.glb_ubcta_col,'$.sku') goods_sn
	FROM(
		SELECT
			a.log_id,
			concat(a.year,a.month,a.day) date,
			a.glb_ubcta_col
		FROM
			stg.zf_pc_event_ubcta_info a
		WHERE
			concat(a.year,a.month,a.day) =${ADD_TIME}
		) a
	JOIN(
		SELECT
			a.log_id,
			a.glb_plf,
			a.cat_id,
			b.country
		FROM(
			SELECT
				log_id,
				glb_plf,
				http_true_client_ip,
				glb_p  cat_id,
				year,
				month,
				day
			FROM
				stg.zf_pc_event_info a
			WHERE
				concat(year,month,day) =${ADD_TIME}
				AND glb_s= 'b01'
				AND a.glb_t = 'ie' 
				AND  a.glb_ubcta != '' 
				AND a.glb_plf in ('pc','m')
			)a
		JOIN
			stg.zaful_ip_region c
		ON
			a.http_true_client_ip = c.ip
		JOIN
			cli_country_num_tmp b
		ON
			concat(a.year,a.month,a.day) = b.date AND a.glb_plf = b.glb_plf AND c.country = b.country
		) b
	ON
		a.log_id = b.log_id
	) tmp
WHERE
	goods_sn != '' AND goods_sn IS NOT NULL
GROUP BY
	date,
	glb_plf,
	cat_id,
	country,
	goods_sn;
	
	
CREATE TABLE IF NOT EXISTS goods_date_exp_cat_country_temp_v2(
	date        string   COMMENT '日期',
	glb_plf     string   COMMENT '平台',
	goods_sn    string   COMMENT '商品SKU',
	cat_id      string   COMMENT '分类',
	country     string   COMMENT '国家',
	num         int      COMMENT '商品曝光数量'
	)
COMMENT "SKU曝光数据统计"
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\u0001' 
LINES TERMINATED BY '\n' 
STORED AS TEXTFILE;
--曝光
INSERT OVERWRITE TABLE goods_date_exp_cat_country_temp_v2
SELECT
	date,
	glb_plf,
	a.goods_sn,
	b.cat_id,
	country,
	num
FROM(
	SELECT
		date,
		glb_plf,
		goods_sn,
		country,
		count(*) num
	FROM(
		SELECT
			a.date,
			b.glb_plf,
			b.country,
			get_json_object(a.glb_ubcta_col,'$.sku') goods_sn
		FROM(
			SELECT
				a.log_id,
				concat(a.year,a.month,a.day) date,
				a.glb_ubcta_col
			FROM
				stg.zf_pc_event_ubcta_info a
			WHERE
				concat(a.year,a.month,a.day) =${ADD_TIME}
			) a
		JOIN(
			SELECT
				a.log_id,
				a.glb_plf,
				b.country
			FROM(
				SELECT
					log_id,
					glb_plf,
					http_true_client_ip,
					year,
					month,
					day
				FROM
					stg.zf_pc_event_info a
				WHERE
					concat(year,month,day) =${ADD_TIME}
					AND glb_s= 'b01'
					AND a.glb_t = 'ie' 
					AND  a.glb_ubcta != '' 
					AND a.glb_plf in ('pc','m')
				)a
			JOIN
				stg.zaful_ip_region c
			ON
				a.http_true_client_ip = c.ip
			JOIN
				cli_country_num_tmp b
			ON
				concat(a.year,a.month,a.day) = b.date AND a.glb_plf = b.glb_plf AND c.country = b.country
			) b
		ON
			a.log_id = b.log_id
		) tmp
	WHERE
		goods_sn != '' AND goods_sn IS NOT NULL
	GROUP BY
		date,
		glb_plf,
		country,
		goods_sn
	)a
JOIN
	stg.zaful_eload_goods b
ON
	a.goods_sn = b.goods_sn;


--点击
CREATE TABLE IF NOT EXISTS goods_date_cli_cat_country_temp(
	date          string   COMMENT '日期',
	glb_plf       string   COMMENT '平台',
	goods_sn      string   COMMENT '商品SKU',
	cat_id        string   COMMENT '分类',
	country       string   COMMENT '国家',
	num           bigint   COMMENT '商品点击数量'
	)
COMMENT "SKU点击数据统计"
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\u0001' 
LINES TERMINATED BY '\n' 
STORED AS TEXTFILE;


INSERT OVERWRITE TABLE goods_date_cli_cat_country_temp
SELECT
	date,
	glb_plf,
	goods_sn,
	cat_id,
	country,
	COUNT(glb_od) click_num
FROM(
	SELECT
		concat(a.year,a.month,a.day) date,
		a.glb_plf,
		get_json_object(a.glb_skuinfo,'$.sku') goods_sn,
		a.glb_od,
		b.country,
		a.cat_id
	FROM(
		SELECT
			glb_skuinfo,
			glb_plf,
			glb_od,
			http_true_client_ip,
			glb_p cat_id,
			year,
			month,
			day
		FROM
			stg.zf_pc_event_info a
		WHERE 
		a.glb_t = 'ic' AND concat(a.year,a.month,a.day) =${ADD_TIME}
		AND glb_s = 'b01'
		AND glb_pm = 'mp'
		AND glb_ubcta not like '%sckw%'
         AND a.glb_skuinfo != '' AND a.glb_plf in ('pc','m')
         and a.glb_x in ('addtobag','sku')
			)a
	JOIN
		stg.zaful_ip_region c
	ON
		a.http_true_client_ip = c.ip
	JOIN
		cli_country_num_tmp b
	ON
		concat(a.year,a.month,a.day) = b.date AND a.glb_plf = b.glb_plf AND c.country = b.country
	) tmp
GROUP BY
	date,
	glb_plf,
	cat_id,
	country,
	goods_sn;
	
	
	
CREATE TABLE IF NOT EXISTS goods_date_cli_cat_country_temp_v2(
	date          string   COMMENT '日期',
	glb_plf       string   COMMENT '平台',
	goods_sn      string   COMMENT '商品SKU',
	cat_id        string   COMMENT '分类',
	country       string   COMMENT '国家',
	num           bigint   COMMENT '商品点击数量'
	)
COMMENT "SKU点击数据统计"
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\u0001' 
LINES TERMINATED BY '\n' 
STORED AS TEXTFILE;


INSERT OVERWRITE TABLE goods_date_cli_cat_country_temp_v2
SELECT
	date,
	glb_plf,
	a.goods_sn,
	b.cat_id,
	country,
	click_num
FROM(
	SELECT
		date,
		glb_plf,
		goods_sn,
		cat_id,
		country,
		COUNT(glb_od) click_num
	FROM(
		SELECT
			concat(a.year,a.month,a.day) date,
			a.glb_plf,
			get_json_object(a.glb_skuinfo,'$.sku') goods_sn,
			a.glb_od,
			b.country,
			a.cat_id
		FROM(
			SELECT
				glb_skuinfo,
				glb_plf,
				glb_od,
				http_true_client_ip,
				glb_p cat_id,
				year,
				month,
				day
			FROM
				stg.zf_pc_event_info a
			WHERE 
			a.glb_t = 'ic' AND concat(a.year,a.month,a.day) =${ADD_TIME}
			AND glb_s = 'b01'
			AND glb_pm = 'mp'
			AND glb_ubcta not like '%sckw%'
			AND a.glb_skuinfo != '' AND a.glb_plf in ('pc','m')
			and a.glb_x in ('addtobag','sku')
				)a
		JOIN
			stg.zaful_ip_region c
		ON
			a.http_true_client_ip = c.ip
		JOIN
			cli_country_num_tmp b
		ON
			concat(a.year,a.month,a.day) = b.date AND a.glb_plf = b.glb_plf AND c.country = b.country
		) tmp
	GROUP BY
		date,
		glb_plf,
		cat_id,
		country,
		goods_sn
	)a
JOIN
	stg.zaful_eload_goods b
ON
	a.goods_sn = b.goods_sn;



--商品加购数
CREATE TABLE IF NOT EXISTS goods_date_cart_cat_country_temp(
	date          string   COMMENT '日期',
	glb_plf       string   COMMENT '平台',
	goods_sn      string   COMMENT '商品SKU',
	cat_id        string   COMMENT '分类',
	country       string   COMMENT '国家',
	num           bigint   COMMENT '商品点击数量'
	)
COMMENT "SKU加购数据统计"
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\u0001' 
LINES TERMINATED BY '\n' 
STORED AS TEXTFILE;

INSERT OVERWRITE TABLE goods_date_cart_cat_country_temp
SELECT
	a.date,
	a.glb_plf,
	a.goods_sn,
	b.cat_id,
	a.country,
	a.num
FROM(
	SELECT
	date,
	glb_plf,
	goods_sn,
	country,
	COUNT(*) num
FROM(
	SELECT
		concat(a.year,a.month,a.day) date,
		a.glb_plf,
		get_json_object(a.glb_skuinfo,'$.sku') goods_sn,
		b.country
	FROM(
		SELECT
			glb_skuinfo,
			glb_plf,
			http_true_client_ip,
			year,
			month,
			day
		FROM
			stg.zf_pc_event_info a
		WHERE 
			concat(a.year,a.month,a.day)=${ADD_TIME}
			AND get_json_object(a.glb_ubcta,'$.fmd') = 'mp'
			AND a.glb_t = 'ic'
			AND a.glb_x = 'ADT'
			AND glb_ubcta not like '%sckw%'
			AND a.glb_plf in ('pc','m')
			)a
	JOIN
		stg.zaful_ip_region c
	ON
		a.http_true_client_ip = c.ip
	JOIN
		cli_country_num_tmp b
	ON
		concat(a.year,a.month,a.day) = b.date AND a.glb_plf = b.glb_plf AND c.country = b.country
	) tmp
GROUP BY
	date,
	glb_plf,
	country,
	goods_sn
	)a
JOIN
	stg.zaful_eload_goods b
ON
	a.goods_sn = b.goods_sn;
	


--商品收藏数	

CREATE TABLE IF NOT EXISTS goods_date_coll_cat_country_temp(
	date          string   COMMENT '日期',
	glb_plf       string   COMMENT '平台',
	goods_sn      string   COMMENT '商品SKU',
	cat_id        string   COMMENT '分类',
	country       string   COMMENT '国家',
	num           bigint   COMMENT '商品点击数量'
	)
COMMENT "SKU收藏数据统计"
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\u0001' 
LINES TERMINATED BY '\n' 
STORED AS TEXTFILE;

INSERT OVERWRITE TABLE goods_date_coll_cat_country_temp
SELECT
	a.date,
	a.glb_plf,
	a.goods_sn,
	b.cat_id,
	a.country,
	a.num
FROM(
	SELECT
		date,
		glb_plf,
		goods_sn,
		country,
		count(*) as num
	FROM
		(SELECT 
			concat(a.year,a.month,a.day) date,
			a.glb_plf,
			b.country,
			get_json_object(a.glb_skuinfo,'$.sku') goods_sn
		FROM
			(SELECT
				glb_skuinfo,
				glb_plf,
				http_true_client_ip,
				year,
				month,
				day
			FROM
				stg.zf_pc_event_info a
			WHERE
				concat(a.year,a.month,a.day) =${ADD_TIME}
				AND get_json_object(a.glb_ubcta,'$.fmd') = 'mp'
				AND a.glb_t = 'ic'
				AND a.glb_x = 'ADF'
				AND glb_ubcta not like '%sckw%'
				AND a.glb_plf in ('pc','m')
				AND a.glb_u <> ''
			)a
		JOIN
			stg.zaful_ip_region c
		ON
			a.http_true_client_ip = c.ip
		JOIN
			cli_country_num_tmp b
		ON
			concat(a.year,a.month,a.day) = b.date AND a.glb_plf = b.glb_plf AND c.country = b.country
	) tmp
	group by
		date,
		glb_plf,
		goods_sn,
		country
	)a
JOIN
	stg.zaful_eload_goods b
ON
	a.goods_sn = b.goods_sn
;



--PC端 M端 订单数量,销售量
CREATE TABLE IF NOT EXISTS goods_order_cat_country_tmp(
    date                  string          COMMENT '日期',
	glb_plf               string          COMMENT '平台',
    goods_sn              STRING          comment "商品sku", 
	cat_id                string          comment "分类",
	country               string          COMMENT '国家',
    order_number          INT             comment "订单数量",
    goods_number          INT             comment "销售量",
	gmv                   double          comment "金额"	
) comment "sku的订单数，销售量"
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\u0001' LINES TERMINATED BY '\n' STORED AS TEXTFILE;


INSERT OVERWRITE TABLE goods_order_cat_country_tmp
SELECT
	a.date,
	a.glb_plf,
	a.goods_sn,
	b.cat_id,
	a.country,
	a.order_number,
	a.goods_number,
	a.gmv
FROM(
	SELECT
		from_unixtime(a.addtime, 'yyyyMMdd') date, 
		b.glb_plf,
		a.goods_sn,
		b.country
		,sum(1) AS order_number
		,sum(a.goods_number) AS goods_number
		,sum(a.goods_number*a.goods_price) gmv
	FROM stg.zaful_eload_order_goods a
	JOIN (
		SELECT
			a.order_id,
			b.glb_plf,
			b.country
		FROM(
			SELECT
				case when (order_sn LIKE 'UU1%' OR   order_sn   LIKE 'U1%' ) then 'pc'
				when (order_sn LIKE 'UL%'  OR   order_sn   LIKE 'UM%' ) then 'm'
				else '' end as  glb_plf,
				pay_ip,
				add_time,
				order_id
			FROM	
				stg.zaful_eload_order_info
			)a
		JOIN
			stg.zaful_ip_region c
		ON
			a.pay_ip = c.ip
		JOIN
			cli_country_num_tmp b
		ON
			from_unixtime(a.add_time, 'yyyyMMdd') = b.date AND a.glb_plf = b.glb_plf AND c.country = b.country
		) b
		on a.order_id = b.order_id
	WHERE a.goods_sn IS NOT NULL
		AND a.goods_sn <> ''
	GROUP BY 
		from_unixtime(a.addtime, 'yyyyMMdd'),
		b.glb_plf,
		b.country,
		a.goods_sn
	)a
JOIN
	stg.zaful_eload_goods b
ON
	a.goods_sn = b.goods_sn

;


CREATE TABLE  IF NOT EXISTS zaful_list_plat_country (
	cat_id            string         COMMENT '分类ID',
	country           string         COMMENT '国家',
	goods_sn          string         COMMENT '商品SKU',
	pv_count          bigint         COMMENT '商品曝光次数',
	ipv_count         bigint         COMMENT '商品点击次数',
	favorite_count    bigint         COMMENT '商品收藏次数',
	bag_count         bigint         COMMENT '商品加购次数',
	order_number      bigint         COMMENT '商品订单次数',
	goods_number      bigint         COMMENT '商品销量',
	gmv               double         COMMENT '金额',
	timestamp         bigint         COMMENT '时间戳',
	date              string         COMMENT '日期',
	plat              string         COMMENT '平台'
	)
COMMENT "SKU行为数据统计"
PARTITIONED BY (pdate string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\u0001' 
LINES TERMINATED BY '\n' 
STORED AS TEXTFILE;	


INSERT OVERWRITE TABLE zaful_list_plat_country partition(pdate=${ADD_TIME})
SELECT
	NVL(t1.cat_id,''),
	NVL(t1.country,''),
	NVL(t1.goods_sn,''),
	NVL(t1.num,0),
	NVL(t2.num,0),
	NVL(t5.num,0),
	NVL(t3.num,0),
	NVL(t6.order_number,0),
	NVL(t6.goods_number,0),
	NVL(to_unix_timestamp(t1.date,"yyyyMMdd"),0),
	NVL(t1.date,''),
	NVL(t1.glb_plf,'')
FROM
	goods_date_exp_cat_country_temp t1
LEFT JOIN
	goods_date_cli_cat_country_temp t2
ON
	t1.date = t2.date AND t1.goods_sn = t2.goods_sn AND t1.glb_plf=t2.glb_plf AND t1.cat_id = t2.cat_id AND t1.country = t2.country
LEFT JOIN
	goods_date_cart_cat_country_temp t3
ON
    t1.date = t3.date AND t1.goods_sn = t3.goods_sn AND t1.glb_plf=t3.glb_plf AND t1.cat_id = t3.cat_id AND t1.country = t3.country
LEFT JOIN
	goods_date_coll_cat_country_temp t5
ON
    t1.date = t5.date AND t1.goods_sn = t5.goods_sn AND t1.glb_plf=t5.glb_plf AND t1.cat_id = t5.cat_id AND t1.country = t5.country
LEFT JOIN
	goods_order_cat_country_tmp t6
ON
    t1.date = t6.date AND t1.goods_sn = t6.goods_sn AND t1.glb_plf=t6.glb_plf AND t1.cat_id = t6.cat_id AND t1.country = t6.country
;



INSERT OVERWRITE TABLE zaful_list_plat_country partition(pdate=${ADD_TIME})
SELECT
	a.cat_id            ,
	NVL(b.code,'')         ,
	a.goods_sn          ,
	a.pv_count          ,
	a.ipv_count         ,
	a.favorite_count    ,
	a.bag_count         ,
	a.order_number      ,
	a.goods_number      ,
	a.timestamp         ,
	a.date              ,
	a.plat              
FROM(
	SELECT
		a.cat_id            ,
		a.country     ,
		a.goods_sn          ,
		a.pv_count          ,
		a.ipv_count         ,
		a.favorite_count    ,
		a.bag_count         ,
		a.order_number      ,
		a.goods_number      ,
		a.timestamp         ,
		a.date              ,
		a.plat              
	FROM
		zaful_list_plat_country a
	WHERE
		pdate=${ADD_TIME}
	)a
JOIN
	cuntryode b
ON
	a.country = b.country
;

CREATE TABLE  IF NOT EXISTS zaful_list_plat_country_v2 (
	cat_id            string         COMMENT '分类ID',
	country           string         COMMENT '国家',
	goods_sn          string         COMMENT '商品SKU',
	pv_count          bigint         COMMENT '商品曝光次数',
	ipv_count         bigint         COMMENT '商品点击次数',
	favorite_count    bigint         COMMENT '商品收藏次数',
	bag_count         bigint         COMMENT '商品加购次数',
	order_number      bigint         COMMENT '商品订单次数',
	goods_number      bigint         COMMENT '商品销量',
	gmv               double         COMMENT '金额',
	timestamp         bigint         COMMENT '时间戳',
	date              string         COMMENT '日期',
	plat              string         COMMENT '平台'
	)
COMMENT "SKU行为数据统计"
PARTITIONED BY (pdate string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\u0001' 
LINES TERMINATED BY '\n' 
STORED AS TEXTFILE;	


INSERT OVERWRITE TABLE zaful_list_plat_country_v2 partition(pdate=${ADD_TIME})
SELECT
	NVL(t1.cat_id,''),
	NVL(t1.country,''),
	NVL(t1.goods_sn,''),
	NVL(t1.num,0),
	NVL(t2.num,0),
	NVL(t5.num,0),
	NVL(t3.num,0),
	NVL(t6.order_number,0),
	NVL(t6.goods_number,0),
	NVL(t6.gmv,0),
	NVL(to_unix_timestamp(t1.date,"yyyyMMdd"),0),
	NVL(t1.date,''),
	NVL(t1.glb_plf,'')
FROM
	goods_date_exp_cat_country_temp_v2 t1
LEFT JOIN
	goods_date_cli_cat_country_temp_v2 t2
ON
	t1.date = t2.date AND t1.goods_sn = t2.goods_sn AND t1.glb_plf=t2.glb_plf AND t1.cat_id = t2.cat_id AND t1.country = t2.country
LEFT JOIN
	goods_date_cart_cat_country_temp t3
ON
    t1.date = t3.date AND t1.goods_sn = t3.goods_sn AND t1.glb_plf=t3.glb_plf AND t1.cat_id = t3.cat_id AND t1.country = t3.country
LEFT JOIN
	goods_date_coll_cat_country_temp t5
ON
    t1.date = t5.date AND t1.goods_sn = t5.goods_sn AND t1.glb_plf=t5.glb_plf AND t1.cat_id = t5.cat_id AND t1.country = t5.country
LEFT JOIN
	goods_order_cat_country_tmp t6
ON
    t1.date = t6.date AND t1.goods_sn = t6.goods_sn AND t1.glb_plf=t6.glb_plf AND t1.cat_id = t6.cat_id AND t1.country = t6.country
;



INSERT OVERWRITE TABLE zaful_list_plat_country_v2 partition(pdate=${ADD_TIME})
SELECT
	a.cat_id            ,
	NVL(b.code,'')         ,
	a.goods_sn          ,
	a.pv_count          ,
	a.ipv_count         ,
	a.favorite_count    ,
	a.bag_count         ,
	a.order_number      ,
	a.goods_number      ,
	a.gmv,
	a.timestamp         ,
	a.date              ,
	a.plat              
FROM(
	SELECT
		a.cat_id            ,
		a.country     ,
		a.goods_sn          ,
		a.pv_count          ,
		a.ipv_count         ,
		a.favorite_count    ,
		a.bag_count         ,
		a.order_number      ,
		a.goods_number      ,
		a.gmv,
		a.timestamp         ,
		a.date              ,
		a.plat              
	FROM
		zaful_list_plat_country_v2 a
	WHERE
		pdate=${ADD_TIME}
	)a
JOIN
	cuntryode b
ON
	a.country = b.country
;
