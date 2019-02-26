--@author Liuqingfan
--@date 2018年6月25日 
--@desc  区域点击超过1000，国家点击超过5000

SET mapred.job.name='click_country_region_data';
set mapred.job.queue.name=root.ai.online; 
SET hive.auto.convert.join=false;
USE dw_zaful_recommend;

--所需数据中间表准备

CREATE TABLE IF NOT EXISTS goods_date_exp_region_temp(
	date        string   COMMENT '日期',
	glb_plf     string   COMMENT '平台',
	goods_sn    string   COMMENT '商品SKU',
	region      string   COMMENT '地区',
	country     string   COMMENT '国家',
	num         int      COMMENT '商品曝光数量'
	)
COMMENT "SKU曝光数据统计"
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\u0001' 
LINES TERMINATED BY '\n' 
STORED AS TEXTFILE;
--曝光
INSERT OVERWRITE TABLE goods_date_exp_region_temp
SELECT
	date,
	glb_plf,
	goods_sn,
	region,
	country,
	count(*) num
FROM(
	SELECT
		a.date,
		b.glb_plf,
		b.region,
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
			concat(a.year,a.month,a.day)  = ${ADD_TIME}
		) a
	JOIN(
		SELECT
			a.log_id,
			a.glb_plf,
			b.country,
			b.region
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
				concat(year,month,day)  = ${ADD_TIME}
				AND a.glb_t = 'ie' AND  a.glb_ubcta != '' AND a.glb_plf in ('pc','m')
			) a
		JOIN
			stg.zaful_ip_region c
		ON
			a.http_true_client_ip = c.ip
		JOIN
			cli_country_region_num_tmp b
		ON
			concat(a.year,a.month,a.day) = b.date AND a.glb_plf = b.glb_plf AND c.region = b.region
		) b
	ON
		a.log_id = b.log_id
	) tmp
WHERE
	goods_sn != '' AND goods_sn IS NOT NULL
GROUP BY
	date,
	glb_plf,
	region,
	country,
	goods_sn;


	
	

--点击
CREATE TABLE IF NOT EXISTS goods_date_cli_region_temp(
	date          string   COMMENT '日期',
	glb_plf       string   COMMENT '平台',
	goods_sn      string   COMMENT '商品SKU',
	region        string   comment '区域',
	country       string   COMMENT '国家',
	num           bigint   COMMENT '商品点击数量'
	)
COMMENT "SKU点击数据统计"
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\u0001' 
LINES TERMINATED BY '\n' 
STORED AS TEXTFILE;
INSERT OVERWRITE TABLE goods_date_cli_region_temp
SELECT
	date,
	glb_plf,
	goods_sn,
	region,
	country,
	COUNT(*) click_num
FROM(
	SELECT
		concat(a.year,a.month,a.day) date,
		a.glb_plf,
		get_json_object(a.glb_skuinfo,'$.sku') goods_sn,
		b.region,
		b.country
	FROM
		(
			SELECT
				glb_plf,
				http_true_client_ip,
				glb_skuinfo,
				year,
				month,
				day
			FROM
				stg.zf_pc_event_info a
			WHERE 
		a.glb_t = 'ic' AND concat(a.year,a.month,a.day)  = ${ADD_TIME}
         AND a.glb_skuinfo != '' AND a.glb_plf in ('pc','m')
         and a.glb_x in ('addtobag','sku')
			) a
	JOIN
		stg.zaful_ip_region c
	ON
		a.http_true_client_ip = c.ip
	JOIN
		cli_country_region_num_tmp b
	ON
		concat(a.year,a.month,a.day) = b.date AND a.glb_plf = b.glb_plf AND c.region = b.region
	) tmp
GROUP BY
	date,
	glb_plf,
	region,
	country,
	goods_sn;



--商品加购数
CREATE TABLE IF NOT EXISTS goods_date_cart_region_temp(
	date          string   COMMENT '日期',
	glb_plf       string   COMMENT '平台',
	goods_sn      string   COMMENT '商品SKU',
	region        string   COMMENT '区域',
	country       string   COMMENT '国家',
	num           bigint   COMMENT '商品点击数量'
	)
COMMENT "SKU加购数据统计"
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\u0001' 
LINES TERMINATED BY '\n' 
STORED AS TEXTFILE;

INSERT OVERWRITE TABLE goods_date_cart_region_temp
SELECT
  date,
  glb_plf,
  goods_sn,
  region,
  country,
  count(*) num
FROM
(
SELECT 
        concat(a.year,a.month,a.day) date,
		a.glb_plf,
		get_json_object(a.glb_skuinfo,'$.sku') goods_sn,
		b.region,
		b.country
        FROM(
			SELECT
				year,
				month,
				day,
				glb_plf,
				glb_skuinfo,
				http_true_client_ip
			FROM
				 stg.zf_pc_event_info a
			WHERE
				concat(a.year,a.month,a.day) = ${ADD_TIME}
  AND a.glb_t = 'ic'
  AND a.glb_x = 'ADT'
   AND a.glb_plf in ('pc','m')
			) a
 JOIN
		stg.zaful_ip_region c
	ON
		a.http_true_client_ip = c.ip
	JOIN
		cli_country_region_num_tmp b
	ON
		concat(a.year,a.month,a.day) = b.date AND a.glb_plf = b.glb_plf AND c.region = b.region
) tmp
group by
  date,
  glb_plf,
  goods_sn,
  region,
  country
;


--商品收藏数	

CREATE TABLE IF NOT EXISTS goods_date_coll_region_temp(
	date          string   COMMENT '日期',
	glb_plf       string   COMMENT '平台',
	goods_sn      string   COMMENT '商品SKU',
	region        string   COMMENT '地域',
	country       string   COMMENT '国家',
	num           bigint   COMMENT '商品点击数量'
	)
COMMENT "SKU收藏数据统计"
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\u0001' 
LINES TERMINATED BY '\n' 
STORED AS TEXTFILE;

INSERT OVERWRITE TABLE goods_date_coll_region_temp
SELECT
	date,
	glb_plf,
    goods_sn,
	region,
	country,
    count(*) as collect_goods_number
FROM
(
  SELECT 
        concat(a.year,a.month,a.day) date,
		a.glb_plf,
		b.region,
		b.country,
		get_json_object(a.glb_skuinfo,'$.sku') goods_sn
    FROM
		(
			SELECT
				year,
				month,
				day,
				glb_plf,
				glb_skuinfo,
				http_true_client_ip
			FROM
				 stg.zf_pc_event_info a
			WHERE
				 concat(a.year,a.month,a.day)  = ${ADD_TIME}
				AND a.glb_t = 'ic'
				AND a.glb_x = 'ADF'
				AND a.glb_plf in ('pc','m')
				AND a.glb_u <> ''
			) a
   JOIN
		stg.zaful_ip_region c
	ON
		a.http_true_client_ip = c.ip
	JOIN
		cli_country_region_num_tmp b
	ON
		concat(a.year,a.month,a.day) = b.date AND a.glb_plf = b.glb_plf AND c.region = b.region
 
) tmp
group by
  date,
  glb_plf,
  goods_sn,
  region,
  country
;




--PC端 M端 订单数量,销售量
CREATE TABLE IF NOT EXISTS goods_order_region_tmp(
    date                  string          COMMENT '日期',
	glb_plf               string          COMMENT '平台',
    goods_sn              STRING          comment "商品sku", 
	region                string          comment '区域',
	country               string          comment "国家",
    order_number          INT             comment "订单数量",
    goods_number          INT             comment "销售量"                
) comment "sku的订单数，销售量"
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\u0001' LINES TERMINATED BY '\n' STORED AS TEXTFILE;




INSERT OVERWRITE TABLE goods_order_region_tmp
SELECT
	from_unixtime(a.addtime, 'yyyyMMdd'), 
	b.glb_plf,
    a.goods_sn,
	b.region,
	b.country
	,sum(1) AS order_number
	,sum(a.goods_number) AS goods_number
FROM stg.zaful_eload_order_goods a
join (
	SELECT
		a.order_id,
		b.glb_plf,
		b.region,
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
		cli_country_region_num_tmp b
	ON
		from_unixtime(a.add_time, 'yyyyMMdd') = b.date AND a.glb_plf = b.glb_plf AND c.region = b.region
	) b
	on a.order_id = b.order_id
WHERE a.goods_sn IS NOT NULL
	AND a.goods_sn <> ''
GROUP BY 
	from_unixtime(a.addtime, 'yyyyMMdd'),
	b.glb_plf,
	b.region,
	b.country,
	a.goods_sn
;



CREATE TABLE  IF NOT EXISTS lr_base_info_region (
	country           string         COMMENT '国家',
	region            string         COMMENT '区域',
	goods_sn          string         COMMENT '商品SKU',
	pv_count          bigint         COMMENT '商品曝光次数',
	ipv_count         bigint         COMMENT '商品点击次数',
	favorite_count    bigint         COMMENT '商品收藏次数',
	bag_count         bigint         COMMENT '商品加购次数',
	order_number      bigint         COMMENT '商品订单次数',
	goods_number      bigint         COMMENT '商品销量',
	timestamp         bigint         COMMENT '时间戳',
	date              string         COMMENT '日期',
	plat              string         COMMENT '平台'
	)
COMMENT "SKU行为数据统计"
PARTITIONED BY (pdate string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\u0001' 
LINES TERMINATED BY '\n' 
STORED AS TEXTFILE;	


INSERT OVERWRITE TABLE lr_base_info_region partition(pdate=${ADD_TIME})
SELECT
	NVL(t1.country,''),
	NVL(t1.region,''),
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
	goods_date_exp_region_temp t1
LEFT JOIN
	goods_date_cli_region_temp t2
ON
	t1.date = t2.date AND t1.goods_sn = t2.goods_sn AND t1.glb_plf=t2.glb_plf AND t1.region = t2.region
LEFT JOIN
	goods_date_cart_region_temp t3
ON
    t1.date = t3.date AND t1.goods_sn = t3.goods_sn AND t1.glb_plf=t3.glb_plf AND t1.region = t3.region
LEFT JOIN
	goods_date_coll_region_temp t5
ON
    t1.date = t5.date AND t1.goods_sn = t5.goods_sn AND t1.glb_plf=t5.glb_plf AND t1.region = t5.region
LEFT JOIN
	goods_order_region_tmp t6
ON
    t1.date = t6.date AND t1.goods_sn = t6.goods_sn AND t1.glb_plf=t6.glb_plf AND t1.region = t6.region
;


CREATE TABLE  IF NOT EXISTS lr_base_info_region_pc (
	country           string         COMMENT '国家',
	region            string         COMMENT '区域',
	goods_sn          string         COMMENT '商品SKU',
	pv_count          bigint         COMMENT '商品曝光次数',
	ipv_count         bigint         COMMENT '商品点击次数',
	favorite_count    bigint         COMMENT '商品收藏次数',
	bag_count         bigint         COMMENT '商品加购次数',
	order_number      bigint         COMMENT '商品订单次数',
	goods_number      bigint         COMMENT '商品销量',
	timestamp         bigint         COMMENT '时间戳',
	date              string         COMMENT '日期',
	plat              string         COMMENT '平台'
	)
COMMENT "PC端SKU行为数据统计"
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\u0001' 
LINES TERMINATED BY '\n' 
STORED AS TEXTFILE;	

INSERT OVERWRITE TABLE lr_base_info_region_pc
SELECT
	NVL(country,''),
	NVL(region,''),
	NVL(goods_sn,''),
	NVL(pv_count,0),
	NVL(ipv_count,0),
	NVL(favorite_count,0),
	NVL(bag_count,0),
	NVL(order_number,0),
	NVL(goods_number,0),
	NVL(timestamp,0),
	NVL(date,''),
	NVL(plat,'')
FROM
	lr_base_info_region
WHERE
	pdate = ${ADD_TIME} AND plat = 'pc' and country != '' and region != '' and region != 'null' and country != 'null' ;
	
	
	
	
CREATE TABLE  IF NOT EXISTS lr_base_info_region_m (
	country           string         COMMENT '国家',
	region            string         COMMENT '区域',
	goods_sn          string         COMMENT '商品SKU',
	pv_count          bigint         COMMENT '商品曝光次数',
	ipv_count         bigint         COMMENT '商品点击次数',
	favorite_count    bigint         COMMENT '商品收藏次数',
	bag_count         bigint         COMMENT '商品加购次数',
	order_number      bigint         COMMENT '商品订单次数',
	goods_number      bigint         COMMENT '商品销量',
	timestamp         bigint         COMMENT '时间戳',
	date              string         COMMENT '日期',
	plat              string         COMMENT '平台'
	)
COMMENT "M端SKU行为数据统计"
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\u0001' 
LINES TERMINATED BY '\n' 
STORED AS TEXTFILE;	

INSERT OVERWRITE TABLE lr_base_info_region_m
SELECT
	NVL(country,''),
	NVL(region,''),
	NVL(goods_sn,''),
	NVL(pv_count,0),
	NVL(ipv_count,0),
	NVL(favorite_count,0),
	NVL(bag_count,0),
	NVL(order_number,0),
	NVL(goods_number,0),
	NVL(timestamp,0),
	NVL(date,''),
	NVL(plat,'')
FROM
	lr_base_info_region
WHERE
	pdate = ${ADD_TIME} AND plat = 'm' and country != '' and region != '' and region != 'null' and country != 'null' ;
	
