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


CREATE TABLE IF NOT EXISTS goods_date_exp_cat_temp(
	date        string   COMMENT '日期',
	glb_plf     string   COMMENT '平台',
	goods_sn    string   COMMENT '商品SKU',
	cat_id      string   COMMENT '分类',
	num         int      COMMENT '商品曝光数量'
	)
COMMENT "SKU曝光数据统计"
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\u0001' 
LINES TERMINATED BY '\n' 
STORED AS TEXTFILE;
--曝光
INSERT OVERWRITE TABLE goods_date_exp_cat_temp
SELECT
	date,
	glb_plf,
	goods_sn,
	cat_id,
	count(*) num
FROM(
	SELECT
		a.date,
		b.glb_plf,
		b.cat_id,
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
			a.cat_id
		FROM(
			SELECT
				log_id,
				glb_plf,
				http_true_client_ip,
				glb_p cat_id,
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
	goods_sn;


	

--点击
CREATE TABLE IF NOT EXISTS goods_date_cli_cat_temp(
	date          string   COMMENT '日期',
	glb_plf       string   COMMENT '平台',
	goods_sn      string   COMMENT '商品SKU',
	cat_id        string   COMMENT '分类',
	num           bigint   COMMENT '商品点击数量'
	)
COMMENT "SKU点击数据统计"
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\u0001' 
LINES TERMINATED BY '\n' 
STORED AS TEXTFILE;
INSERT OVERWRITE TABLE goods_date_cli_cat_temp
SELECT
	date,
	glb_plf,
	goods_sn,
	cat_id,
	COUNT(glb_od) click_num
FROM(
	SELECT
		concat(a.year,a.month,a.day) date,
		a.glb_plf,
		get_json_object(a.glb_skuinfo,'$.sku') goods_sn,
		a.glb_od,
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
	) tmp
GROUP BY
	date,
	glb_plf,
	cat_id,
	goods_sn;



--商品加购数
CREATE TABLE IF NOT EXISTS goods_date_cart_cat_temp(
	date          string   COMMENT '日期',
	glb_plf       string   COMMENT '平台',
	goods_sn      string   COMMENT '商品SKU',
	cat_id        string   COMMENT '国家',
	num           bigint   COMMENT '商品点击数量'
	)
COMMENT "SKU加购数据统计"
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\u0001' 
LINES TERMINATED BY '\n' 
STORED AS TEXTFILE;

INSERT OVERWRITE TABLE goods_date_cart_cat_temp
SELECT
	a.date,
	a.glb_plf,
	a.goods_sn,
	b.cat_id,
	a.num
FROM(
	SELECT
		date,
	glb_plf,
	goods_sn,
	sum(num) num
	FROM
		(SELECT 
			concat(a.year,a.month,a.day) date,
			a.glb_plf,
			get_json_object(a.glb_skuinfo,'$.sku') goods_sn,
			a.num
		FROM
			(
			SELECT
				glb_skuinfo,
				glb_plf,
				get_json_object(a.glb_skuinfo,'$.pam') num,
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
		WHERE
			num > 0
	) t1
	group by
	date,
	glb_plf,
	goods_sn
	)a
JOIN
	stg.zaful_eload_goods b
ON
	a.goods_sn = b.goods_sn;
	
	



--商品收藏数	

CREATE TABLE IF NOT EXISTS goods_date_coll_cat_temp(
	date          string   COMMENT '日期',
	glb_plf       string   COMMENT '平台',
	goods_sn      string   COMMENT '商品SKU',
	cat_id        string   COMMENT '国家',
	num           bigint   COMMENT '商品点击数量'
	)
COMMENT "SKU收藏数据统计"
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\u0001' 
LINES TERMINATED BY '\n' 
STORED AS TEXTFILE;

INSERT OVERWRITE TABLE goods_date_coll_cat_temp
SELECT
	a.date,
	a.glb_plf,
	a.goods_sn,
	b.cat_id,
	a.num
FROM(
	SELECT
		date,
		glb_plf,
		goods_sn,
		count(glb_od) as num
	FROM
		(SELECT 
			concat(a.year,a.month,a.day) date,
			a.glb_plf,
			a.glb_od,
			get_json_object(a.glb_skuinfo,'$.sku') goods_sn
		FROM
			(
			SELECT
				glb_skuinfo,
				glb_plf,
				glb_od,
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
		) tmp
	group by
	date,
	glb_plf,
	goods_sn
	)a
JOIN
	stg.zaful_eload_goods b
ON
	a.goods_sn = b.goods_sn
;





--PC端 M端 订单数量,销售量
CREATE TABLE IF NOT EXISTS goods_order_cat_tmp(
    date                  string          COMMENT '日期',
	glb_plf               string          COMMENT '平台',
    goods_sn              STRING          comment "商品sku", 
	cat_id                string          comment "国家",
    order_number          INT             comment "订单数量",
    goods_number          INT             comment "销售量"                
) comment "sku的订单数，销售量"
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\u0001' LINES TERMINATED BY '\n' STORED AS TEXTFILE;




INSERT OVERWRITE TABLE goods_order_cat_tmp
SELECT
	a.date,
	a.glb_plf,
	a.goods_sn,
	b.cat_id,
	a.order_number,
	a.goods_number
FROM(
	SELECT
		from_unixtime(a.addtime, 'yyyyMMdd') date, 
		b.glb_plf,
		a.goods_sn
		,sum(1) AS order_number
		,sum(a.goods_number) AS goods_number
	FROM stg.zaful_eload_order_goods a
	join (
		SELECT
			a.order_id,
			a.glb_plf
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
		) b
		on a.order_id = b.order_id
	WHERE a.goods_sn IS NOT NULL
		AND a.goods_sn <> ''
	GROUP BY 
		from_unixtime(a.addtime, 'yyyyMMdd'),
		b.glb_plf,
		a.goods_sn
	)a
JOIN
	stg.zaful_eload_goods b
ON
	a.goods_sn = b.goods_sn

;


CREATE TABLE  IF NOT EXISTS zaful_list_plat (
	cat_id            string         COMMENT '分类ID',
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


INSERT OVERWRITE TABLE zaful_list_plat partition(pdate=${ADD_TIME})
SELECT
	NVL(t1.cat_id,''),
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
	goods_date_exp_cat_temp t1
LEFT JOIN
	goods_date_cli_cat_temp t2
ON
	t1.date = t2.date AND t1.goods_sn = t2.goods_sn AND t1.glb_plf=t2.glb_plf AND t1.cat_id = t2.cat_id
LEFT JOIN
	goods_date_cart_cat_temp t3
ON
    t1.date = t3.date AND t1.goods_sn = t3.goods_sn AND t1.glb_plf=t3.glb_plf AND t1.cat_id = t3.cat_id
LEFT JOIN
	goods_date_coll_cat_temp t5
ON
    t1.date = t5.date AND t1.goods_sn = t5.goods_sn AND t1.glb_plf=t5.glb_plf AND t1.cat_id = t5.cat_id
LEFT JOIN
	goods_order_cat_tmp t6
ON
    t1.date = t6.date AND t1.goods_sn = t6.goods_sn AND t1.glb_plf=t6.glb_plf AND t1.cat_id = t6.cat_id
;

