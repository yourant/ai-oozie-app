
--@author ZhanRui
--@date 2018年9月25日 
--@desc  gb sku 指标数据统计

SET mapred.job.name=gb_sku_info_report;
set mapred.job.queue.name=root.ai.offline;
SET mapred.max.split.size=128000000;
SET mapred.min.split.size=32000000;
SET mapred.min.split.size.per.node=32000000;
SET mapred.min.split.size.per.rack=32000000;
SET hive.exec.reducers.bytes.per.reducer = 128000000;
SET hive.merge.mapfiles=true;
SET hive.merge.mapredfiles= true;
SET hive.input.format=org.apache.hadoop.hive.ql.io.CombineHiveInputFormat;
SET hive.merge.size.per.task=256000000;
SET hive.exec.parallel = true; 


--结果表
CREATE TABLE  IF NOT EXISTS dw_proj.gb_sku_info (
  plat              string         COMMENT '平台',
	goods_sn          string         COMMENT '商品SKU',
	pv_count          bigint         COMMENT '商品曝光次数',
	ipv_count         bigint         COMMENT '商品点击次数',
	favorite_count    bigint         COMMENT '商品收藏次数',
	bag_count         bigint         COMMENT '商品加购次数',
	order_number      bigint         COMMENT '商品订单次数',
	goods_number      bigint         COMMENT '商品销量',
	timestamp         bigint         COMMENT '时间戳',
	date              string         COMMENT '日期'
	)
COMMENT "SKU行为数据统计"
PARTITIONED BY (pdate string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\u0001' 
LINES TERMINATED BY '\n' 
STORED AS TEXTFILE;	

---曝光
CREATE TABLE IF NOT EXISTS dw_proj.gb_sku_exp_tmp(
	date        string   COMMENT '日期',
	goods_sn    string   COMMENT '商品SKU',
	exp_num     bigint   COMMENT '商品曝光数量',
    glb_plf     string   COMMENT '平台'
	)
COMMENT "SKU曝光数据统计"
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\u0001' 
LINES TERMINATED BY '\n' 
STORED AS TEXTFILE;

--点击
CREATE TABLE IF NOT EXISTS dw_proj.gb_sku_cli_tmp(
	date          string   COMMENT '日期',
	goods_sn      string   COMMENT '商品SKU',
	click_num     bigint   COMMENT '商品点击数量',
    glb_plf     string   COMMENT '平台'
	)
COMMENT "SKU点击数据统计"
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\u0001' 
LINES TERMINATED BY '\n' 
STORED AS TEXTFILE;

--加购
CREATE TABLE IF NOT EXISTS dw_proj.gb_sku_cart_tmp(
	date          string   COMMENT '日期',
	goods_sn      string   COMMENT '商品SKU',
	cart_goods_number     bigint   COMMENT '加购物车数量',
    glb_plf     string   COMMENT '平台'
	)
COMMENT "SKU加购物车数量"
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\u0001' 
LINES TERMINATED BY '\n' 
STORED AS TEXTFILE;

--收藏
CREATE TABLE IF NOT EXISTS dw_proj.gb_sku_coll_tmp(
	date          string   COMMENT '日期',
	goods_sn      string   COMMENT '商品SKU',
	collect_goods_number     bigint   COMMENT '加收藏夹数量',
    glb_plf     string   COMMENT '平台'
	)
COMMENT "SKU加收藏夹数量"
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\u0001' 
LINES TERMINATED BY '\n' 
STORED AS TEXTFILE;

--订单销售
CREATE TABLE IF NOT EXISTS dw_proj.gb_sku_order_tmp(
    date                  string          COMMENT '日期',
    goods_sn              STRING          comment "商品sku", 
    order_number          INT             comment "订单数量",
    goods_number          INT             comment "销售量",
    glb_plf               string   COMMENT '平台'                
) comment "SKU订单数，销售量"
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\u0001' 
LINES TERMINATED BY '\n' 
STORED AS TEXTFILE;



--商品曝光数
INSERT OVERWRITE TABLE dw_proj.gb_sku_exp_tmp
SELECT
  m.date,
  m.sku,
  count(*) AS exp_num,
  n.glb_plf
FROM
  (
    SELECT
      log_id,
      get_json_object(glb_ubcta_json, '$.sku') as sku,
      concat(year,month,day) as date
    FROM
      stg.stg_gb_pc_event_info_ubcta
    WHERE
      concat(year,month,day) = '${ADD_TIME}'
      AND get_json_object(glb_ubcta_json, '$.sku') <> ''
  ) m
  INNER JOIN (
    SELECT
      log_id,
      glb_plf
    FROM
      stg.gb_pc_event_info
    WHERE
      concat(year,month,day) = '${ADD_TIME}'
      AND glb_t = 'ie'
      AND glb_ubcta <> ''
  ) n ON m.log_id = n.log_id
group by
  m.date,
  m.sku,
  n.glb_plf;



  
--商品点击数
INSERT OVERWRITE TABLE dw_proj.gb_sku_cli_tmp
SELECT
  concat(year,month,day),
  get_json_object(glb_skuinfo, '$.sku'),
  count(*) AS click_num,
  glb_plf
FROM
  stg.gb_pc_event_info
WHERE
  concat(year,month,day) = '${ADD_TIME}'
  AND glb_t = 'ic'
  AND glb_skuinfo <> ''
  AND glb_ubcta <> ''
group by
  concat(year,month,day),
  get_json_object(glb_skuinfo, '$.sku'),
  glb_plf
  ;



--商品加购数
INSERT OVERWRITE TABLE dw_proj.gb_sku_cart_tmp
SELECT
  concat(year,month,day),
  get_json_object(glb_skuinfo, '$.sku'),
  count(*) AS cart_num,
  glb_plf
FROM
  stg.gb_pc_event_info
WHERE
  concat(year,month,day) = '${ADD_TIME}'
  AND glb_t = 'ic'
  AND glb_x = 'ADT'
group by
  concat(year,month,day),
  get_json_object(glb_skuinfo, '$.sku'),
  glb_plf
  ;



--商品收藏数	
INSERT OVERWRITE TABLE dw_proj.gb_sku_coll_tmp
SELECT
  concat(year,month,day),
  get_json_object(glb_skuinfo, '$.sku'),
  count(*) as collect_num,
  glb_plf
FROM
  stg.gb_pc_event_info
WHERE
  concat(year,month,day) = '${ADD_TIME}'
  AND glb_t = 'ic'
  AND glb_x = 'ADF'
  AND glb_u <> ''
group by
  concat(year,month,day),
  get_json_object(glb_skuinfo, '$.sku'),
  glb_plf
  ;

--订单数、下单商品数
INSERT OVERWRITE TABLE dw_proj.gb_sku_order_tmp
SELECT
	x.date,
	p.goods_sn,
	sum(1) AS order_number,
	sum(p.qty) AS goods_number,
	m.glb_plf
FROM
	(
		SELECT
			order_sn,
			from_unixtime(created_time, 'yyyyMMdd') AS date
		FROM
			stg.gb_order_order_info
		WHERE
			from_unixtime(created_time, 'yyyyMMdd') = '${ADD_TIME}'
	) x
JOIN stg.gb_order_order_goods p ON x.order_sn = p.order_sn
JOIN (
	SELECT
		CASE
	WHEN platform = '1' THEN
		'pc'
	WHEN platform = '2' THEN
		'm'
	ELSE
		'app'
	END AS glb_plf,
	order_sn
FROM
	stg.gb_order_info_extend
) m ON x.order_sn = m.order_sn
GROUP BY
	x.date,
	p.goods_sn,
	m.glb_plf
;


INSERT OVERWRITE TABLE dw_proj.gb_sku_info partition(pdate = '${ADD_TIME}')
SELECT
    t1.glb_plf,
	NVL(t1.goods_sn,''),
	NVL(t1.exp_num,0),
	NVL(t2.click_num,0),
	NVL(t4.collect_goods_number,0),
	NVL(t3.cart_goods_number,0),
  NVL(t5.order_number,0),
  NVL(t5.goods_number,0),
	NVL(to_unix_timestamp(t1.date,"yyyyMMdd"),0),
	NVL(t1.date,'')
FROM
    (SELECT goods_sn,date,exp_num,glb_plf FROM
	dw_proj.gb_sku_exp_tmp ) t1
LEFT JOIN
    (SELECT goods_sn,date,click_num,glb_plf FROM
	dw_proj.gb_sku_cli_tmp ) t2
ON
	t1.date = t2.date AND t1.goods_sn = t2.goods_sn AND t1.glb_plf = t2.glb_plf
LEFT JOIN
    (SELECT goods_sn,date,cart_goods_number,glb_plf FROM
	dw_proj.gb_sku_cart_tmp ) t3
ON
    t1.date = t3.date AND t1.goods_sn = t3.goods_sn AND t1.glb_plf = t3.glb_plf
LEFT JOIN
    (SELECT goods_sn,date,collect_goods_number,glb_plf FROM
	dw_proj.gb_sku_coll_tmp ) t4
ON
    t1.date = t4.date AND t1.goods_sn = t4.goods_sn  AND t1.glb_plf = t4.glb_plf
LEFT JOIN
    (SELECT goods_sn,date,order_number,goods_number,glb_plf FROM
	dw_proj.gb_sku_order_tmp ) t5
ON
    t1.date = t5.date AND t1.goods_sn = t5.goods_sn  AND t1.glb_plf = t5.glb_plf
;