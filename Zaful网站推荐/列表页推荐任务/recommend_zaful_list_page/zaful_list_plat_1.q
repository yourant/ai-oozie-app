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


CREATE TABLE IF NOT EXISTS goods_date_exp_cat_temp_v2(
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
INSERT OVERWRITE TABLE goods_date_exp_cat_temp_v2
SELECT
	date,
	glb_plf,
	a.goods_sn,
	b.cat_id,
	count(*) num
FROM(
	SELECT
		a.date,
		b.glb_plf,
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
			a.glb_plf
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
		) b
	ON
		a.log_id = b.log_id
	) a
JOIN
	stg.zaful_eload_goods b
ON
	a.goods_sn = b.goods_sn
WHERE
	a.goods_sn != '' AND a.goods_sn IS NOT NULL
GROUP BY
	date,
	glb_plf,
	b.cat_id,
	a.goods_sn;


	

--点击
CREATE TABLE IF NOT EXISTS goods_date_cli_cat_temp_v2(
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
INSERT OVERWRITE TABLE goods_date_cli_cat_temp_v2
SELECT
	date,
	glb_plf,
	a.goods_sn,
	b.cat_id,
	COUNT(glb_od) click_num
FROM(
	SELECT
		concat(a.year,a.month,a.day) date,
		a.glb_plf,
		get_json_object(a.glb_skuinfo,'$.sku') goods_sn,
		a.glb_od
	FROM(
		SELECT
			glb_skuinfo,
			glb_plf,
			glb_od,
			http_true_client_ip,
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
	)a
JOIN
	stg.zaful_eload_goods b
ON
	a.goods_sn = b.goods_sn
GROUP BY
	date,
	glb_plf,
	b.cat_id,
	a.goods_sn;



--商品加购数
CREATE TABLE IF NOT EXISTS goods_date_cart_cat_temp_v2(
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

INSERT OVERWRITE TABLE goods_date_cart_cat_temp_v2
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

CREATE TABLE IF NOT EXISTS goods_date_coll_cat_temp_v2(
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

INSERT OVERWRITE TABLE goods_date_coll_cat_temp_v2
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



--中间表report_sku_user_tmp：取sku,user_id
INSERT OVERWRITE TABLE cat_sku_user_country_tmp 
SELECT
  m.sku,
  n.glb_u,
  m.glb_plf,
  m.geoip_country_name,
  m.glb_dc
FROM
  (
    SELECT
      glb_od,
      regexp_extract(glb_skuinfo, '(.*?sku":")([0-9a-zA-Z]*)(".*?)', 2) AS sku,
      glb_plf,
      geoip_country_name,
      glb_dc
    FROM
      stg.zf_pc_event_info
    WHERE
      concat( year, month, day) between ${ADD_TIME_W} and ${ADD_TIME}
      AND glb_t = 'ic'
      AND glb_x = 'ADT'
      and get_json_object(glb_ubcta, '$.fmd')='mp'
      and get_json_object(glb_ubcta, '$.sckw') is null
      AND glb_skuinfo <> ''
      AND glb_ubcta <> ''
  ) m
  INNER JOIN dw_zaful_recommend.zaful_od_u_map n ON m.glb_od = n.glb_od
GROUP BY
  m.glb_plf,
  m.sku,
  n.glb_u,
  m.geoip_country_name,
  m.glb_dc;



--中间表report_sku_user_tmp：取sku,user_id
INSERT OVERWRITE TABLE cat_sku_user_tmp
SELECT
  p.goods_sn,
  x.user_id,
  x.order_status,
  p.goods_price * p.goods_number as pay_amount,
  q.cat_id,
  p.goods_number
FROM
  (
    SELECT
      order_id,
      user_id,
      add_time,
      order_status
    FROM
      stg.zaful_eload_order_info
    WHERE
      from_unixtime(add_time, 'yyyyMMdd') = '${ADD_TIME}' and order_status not in ('0','11')
  ) x
  JOIN stg.zaful_eload_order_goods p ON x.order_id = p.order_id
  JOIN stg.zaful_eload_goods q ON p.goods_sn = q.goods_sn
group by
  p.goods_sn,
  x.user_id,
  x.order_status,
  p.goods_price * p.goods_number,
  q.cat_id,
  p.goods_number
;


--商品下单数
INSERT OVERWRITE TABLE goods_order_cat_tmp_v2
SELECT
	'${ADD_TIME}',
	x1.glb_plf,
	x2.goods_sn,
	x2.cat_id,
  count(*) AS order_num,
  sum(goods_number) goods_number,
  sum(pay_amount) pay_amount
from
  cat_sku_user_country_tmp x1
  INNER JOIN cat_sku_user_tmp x2 ON x1.glb_u = x2.user_id
  AND x1.sku = x2.goods_sn
group by
  x1.glb_plf,
  x2.cat_id,
  x2.goods_sn;

--PC端 M端 订单数量,销售量
CREATE TABLE IF NOT EXISTS goods_order_cat_tmp_v2(
    date                  string          COMMENT '日期',
	glb_plf               string          COMMENT '平台',
    goods_sn              STRING          comment "商品sku", 
	cat_id                string          comment "国家",
    order_number          INT             comment "订单数量",
    goods_number          INT             comment "销售量" ,
	gmv                   double          comment "金额"
) comment "sku的订单数，销售量"
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\u0001' LINES TERMINATED BY '\n' STORED AS TEXTFILE;






CREATE TABLE  IF NOT EXISTS zaful_list_plat_v2 (
	cat_id            string         COMMENT '分类ID',
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


INSERT OVERWRITE TABLE zaful_list_plat_v2 partition(pdate=${ADD_TIME})
SELECT
	NVL(t1.cat_id,''),
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
	goods_date_exp_cat_temp_v2 t1
LEFT JOIN
	goods_date_cli_cat_temp_v2 t2
ON
	t1.date = t2.date AND t1.goods_sn = t2.goods_sn AND t1.glb_plf=t2.glb_plf AND t1.cat_id = t2.cat_id
LEFT JOIN
	goods_date_cart_cat_temp_v2 t3
ON
    t1.date = t3.date AND t1.goods_sn = t3.goods_sn AND t1.glb_plf=t3.glb_plf AND t1.cat_id = t3.cat_id
LEFT JOIN
	goods_date_coll_cat_temp_v2 t5
ON
    t1.date = t5.date AND t1.goods_sn = t5.goods_sn AND t1.glb_plf=t5.glb_plf AND t1.cat_id = t5.cat_id
LEFT JOIN
	goods_order_cat_tmp_v2 t6
ON
    t1.date = t6.date AND t1.goods_sn = t6.goods_sn AND t1.glb_plf=t6.glb_plf AND t1.cat_id = t6.cat_id
;


