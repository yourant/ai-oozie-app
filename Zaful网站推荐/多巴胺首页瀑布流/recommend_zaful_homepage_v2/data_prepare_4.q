--@author ZhanRui
--@date 2018年6月28日 
--@desc  算法所需数据导入mongo PC/M 每个SKU的统计信息

SET mapred.job.name=lr_base_info4;
set mapred.job.queue.name=root.ai.offline; 
SET mapred.max.split.size=128000000;
SET mapred.min.split.size=128000000;
SET mapred.min.split.size.per.node=128000000;
SET mapred.min.split.size.per.rack=128000000;
SET hive.exec.reducers.bytes.per.reducer = 128000000;
SET hive.merge.mapfiles=true;
SET hive.merge.mapredfiles= true;
SET hive.merge.size.per.task=256000000;
SET hive.exec.parallel = true; 
SET hive.auto.convert.join=false;

USE dw_zaful_report;


CREATE TABLE  IF NOT EXISTS lr_base_info (
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

---曝光
CREATE TABLE IF NOT EXISTS goods_date_exp_temp(
	date        string   COMMENT '日期',
	glb_plf     string   COMMENT '平台',
	goods_sn    string   COMMENT '商品SKU',
	exp_num     bigint   COMMENT '商品曝光数量'
	)
COMMENT "SKU曝光数据统计"
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\u0001' 
LINES TERMINATED BY '\n' 
STORED AS TEXTFILE;

--点击
CREATE TABLE IF NOT EXISTS goods_date_cli_temp(
	date          string   COMMENT '日期',
	glb_plf       string   COMMENT '平台',
	goods_sn      string   COMMENT '商品SKU',
	click_num     bigint   COMMENT '商品点击数量'
	)
COMMENT "SKU点击数据统计"
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\u0001' 
LINES TERMINATED BY '\n' 
STORED AS TEXTFILE;

--加购
CREATE TABLE IF NOT EXISTS goods_date_cart_temp(
	date          string   COMMENT '日期',
	glb_plf     string   COMMENT '平台',
	goods_sn      string   COMMENT '商品SKU',
	cart_goods_number     bigint   COMMENT '加购物车数量'
	)
COMMENT "SKU加购物车数量"
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\u0001' 
LINES TERMINATED BY '\n' 
STORED AS TEXTFILE;

--收藏
CREATE TABLE IF NOT EXISTS goods_date_coll_temp(
	date          string   COMMENT '日期',
	glb_plf       string   COMMENT '平台',
	goods_sn      string   COMMENT '商品SKU',
	collect_goods_number     bigint   COMMENT '加收藏夹数量'
	)
COMMENT "SKU加收藏夹数量"
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\u0001' 
LINES TERMINATED BY '\n' 
STORED AS TEXTFILE;

--订单销售
CREATE TABLE IF NOT EXISTS goods_order_tmp(
    date                  string          COMMENT '日期',
	glb_plf               string          COMMENT '平台',
    goods_sn              STRING          comment "商品sku", 
    order_number          INT             comment "订单数量",
    goods_number          INT             comment "销售量"                
) comment "PC端每一天sku的订单数，销售量，销售金额"
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\u0001' LINES TERMINATED BY '\n' STORED AS TEXTFILE;

--所需数据中间表准备
--曝光
INSERT OVERWRITE TABLE goods_date_exp_temp
SELECT
	date,
	glb_plf,
	goods_sn,
	count(*) exp_num
FROM(
	SELECT
		a.date,
		b.glb_plf,
		get_json_object(a.glb_ubcta_col,'$.sku') goods_sn
	FROM(
		SELECT
			log_id,
			concat(year,month,day) date,
			glb_ubcta_col
		FROM
			stg.zf_pc_event_ubcta_info
		WHERE
			concat(year,month,day) = '${ADD_TIME}'
		) a
	JOIN(
		SELECT
			log_id,
			glb_plf
		FROM
			stg.zf_pc_event_info
		WHERE
		    concat(year,month,day) = '${ADD_TIME}'
            AND glb_t = 'ie' AND  glb_ubcta != '' AND glb_plf in ('pc','m')
		) b
	ON
		a.log_id = b.log_id
	) tmp
WHERE
	goods_sn != '' AND goods_sn IS NOT NULL
GROUP BY
	date,
	glb_plf,
	goods_sn;

--点击
INSERT OVERWRITE TABLE goods_date_cli_temp
SELECT
	date,
	glb_plf,
	goods_sn,
	COUNT(*) click_num
FROM(
	SELECT
		concat(year,month,day) date,
		glb_plf,
		get_json_object(a.glb_skuinfo,'$.sku') goods_sn
	FROM
		stg.zf_pc_event_info a
	WHERE 
		glb_t = 'ic' AND concat(year,month,day) = '${ADD_TIME}'
         AND glb_skuinfo != '' AND glb_plf in ('pc','m') 
         and glb_x in ('addtobag','sku')
	) tmp
GROUP BY
	date,
	glb_plf,
	goods_sn;



--商品加购数
INSERT OVERWRITE TABLE goods_date_cart_temp
SELECT
  date,
  glb_plf,
  goods_sn,
  count(*) AS cart_goods_number
FROM
(
SELECT 
        concat(year,month,day) date,
		glb_plf,
		get_json_object(a.glb_skuinfo,'$.sku') goods_sn
        FROM
  stg.zf_pc_event_info a
WHERE
  concat(year,month,day) = '${ADD_TIME}'
  AND glb_t = 'ic'
  AND glb_x = 'ADT'
   AND glb_plf in ('pc','m') 
) tmp
group by
  date,
  glb_plf,
  goods_sn
;


--商品收藏数	
INSERT OVERWRITE TABLE goods_date_coll_temp
SELECT
	date,
	glb_plf,
  goods_sn,
  count(*) as collect_goods_number
FROM
(
  SELECT 
        concat(year,month,day) date,
		glb_plf,
		get_json_object(a.glb_skuinfo,'$.sku') goods_sn
    FROM
  stg.zf_pc_event_info a
WHERE
  concat(year,month,day) = '${ADD_TIME}'
  AND glb_t = 'ic'
  AND glb_x = 'ADF'
  AND glb_plf in ('pc','m')
  AND glb_u <> ''
) tmp
group by
  date,
  glb_plf,
  goods_sn
;




--PC端 M端 订单数量,销售量
INSERT OVERWRITE TABLE goods_order_tmp
SELECT
	from_unixtime(addtime, 'yyyyMMdd') 
	,case when (b.order_sn LIKE 'UU1%' OR   b.order_sn   LIKE 'U1%' ) then 'pc'
	      when (b.order_sn LIKE 'UL%'  OR   b.order_sn   LIKE 'UM%' ) then 'm'
	 else '' end as  glb_plf
    ,a.goods_sn
	,sum(1) AS order_number
	,sum(a.goods_number) AS goods_number
FROM stg.zaful_eload_order_goods a
join stg.zaful_eload_order_info  b
on a.order_id = b.order_id
WHERE a.goods_sn IS NOT NULL
	AND a.goods_sn <> ''
	AND (b.order_sn LIKE 'UU1%' OR   b.order_sn   LIKE 'U1%'  OR
    b.order_sn LIKE 'UL%'  OR   b.order_sn   LIKE 'UM%')
GROUP BY goods_sn
	,from_unixtime(addtime, 'yyyyMMdd')
	,case when (b.order_sn LIKE 'UU1%' OR   b.order_sn   LIKE 'U1%' ) then 'pc'
	      when (b.order_sn LIKE 'UL%'  OR   b.order_sn   LIKE 'UM%' ) then 'm'
	 else '' end
;


INSERT OVERWRITE TABLE lr_base_info partition(pdate = '${ADD_TIME}')
SELECT
	NVL(t1.goods_sn,''),
	NVL(t1.exp_num,0),
	NVL(t2.click_num,0),
	NVL(t5.collect_goods_number,0),
	NVL(t3.cart_goods_number,0),
	NVL(t6.order_number,0),
	NVL(t6.goods_number,0),
	NVL(to_unix_timestamp(t1.date,"yyyyMMdd"),0),
	NVL(t1.date,''),
	NVL(t1.glb_plf,'')
FROM
	goods_date_exp_temp t1
LEFT JOIN
	goods_date_cli_temp t2
ON
	t1.date = t2.date AND t1.goods_sn = t2.goods_sn AND t1.glb_plf=t2.glb_plf
LEFT JOIN
	goods_date_cart_temp t3
ON
    t1.date = t3.date AND t1.goods_sn = t3.goods_sn AND t1.glb_plf=t3.glb_plf
LEFT JOIN
	goods_date_coll_temp t5
ON
    t1.date = t5.date AND t1.goods_sn = t5.goods_sn AND t1.glb_plf=t5.glb_plf
LEFT JOIN
	goods_order_tmp t6
ON
    t1.date = t6.date AND t1.goods_sn = t6.goods_sn AND t1.glb_plf=t6.glb_plf
;