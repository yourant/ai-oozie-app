
--@author ZhanRui
--@date 2018年7月6日 
--@desc  算法所需数据导入mongo PC/M 加拿大和法国首页推荐位每个SKU的统计信息 英语站1301

SET mapred.job.name=lr_base_mrinfo;
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

USE tmp;

--结果表
CREATE TABLE  IF NOT EXISTS lr_base_mrinfo (
    plat              string         COMMENT '平台',
	goods_sn          string         COMMENT '商品SKU',
	pv_count          bigint         COMMENT '商品曝光次数',
	ipv_count         bigint         COMMENT '商品点击次数',
	favorite_count    bigint         COMMENT '商品收藏次数',
	bag_count         bigint         COMMENT '商品加购次数',
	timestamp         bigint         COMMENT '时间戳',
	date              string         COMMENT '日期'
	)
COMMENT "SKU行为数据统计"
PARTITIONED BY (pdate string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\u0001' 
LINES TERMINATED BY '\n' 
STORED AS TEXTFILE;	

---曝光
CREATE TABLE IF NOT EXISTS goods_date_exp_mrtemp(
	date        string   COMMENT '日期',
	goods_sn    string   COMMENT '商品SKU',
	exp_num     bigint   COMMENT '商品曝光数量',
    glb_plf     string   COMMENT '平台',
    mrlc        string   COMMENT '推荐位'
	)
COMMENT "推荐位SKU曝光数据统计"
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\u0001' 
LINES TERMINATED BY '\n' 
STORED AS TEXTFILE;

--点击
CREATE TABLE IF NOT EXISTS goods_date_cli_mrtemp(
	date          string   COMMENT '日期',
	goods_sn      string   COMMENT '商品SKU',
	click_num     bigint   COMMENT '商品点击数量',
    glb_plf     string   COMMENT '平台',
    mrlc        string   COMMENT '推荐位'
	)
COMMENT "推荐位SKU点击数据统计"
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\u0001' 
LINES TERMINATED BY '\n' 
STORED AS TEXTFILE;

--加购
CREATE TABLE IF NOT EXISTS goods_date_cart_mrtemp(
	date          string   COMMENT '日期',
	goods_sn      string   COMMENT '商品SKU',
	cart_goods_number     bigint   COMMENT '加购物车数量',
    glb_plf     string   COMMENT '平台',
    mrlc        string   COMMENT '推荐位'
	)
COMMENT "推荐位SKU加购物车数量"
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\u0001' 
LINES TERMINATED BY '\n' 
STORED AS TEXTFILE;

--收藏
CREATE TABLE IF NOT EXISTS goods_date_coll_mrtemp(
	date          string   COMMENT '日期',
	goods_sn      string   COMMENT '商品SKU',
	collect_goods_number     bigint   COMMENT '加收藏夹数量',
    glb_plf     string   COMMENT '平台',
    mrlc        string   COMMENT '推荐位'
	)
COMMENT "推荐位SKU加收藏夹数量"
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\u0001' 
LINES TERMINATED BY '\n' 
STORED AS TEXTFILE;




--曝光
INSERT OVERWRITE TABLE goods_date_exp_mrtemp
SELECT
	date,
	goods_sn,
	count(*) exp_num,
    glb_plf,
    mrlc
FROM(
	SELECT
		a.date,
		get_json_object(a.glb_ubcta_col,'$.sku') as goods_sn,
        get_json_object(a.glb_ubcta_col,'$.mrlc') as mrlc,
        b.glb_plf
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
            and geoip_country_name in ('Canada','France')
            and glb_b = 'a'
			and glb_dc='1301'
		) b
	ON
		a.log_id = b.log_id
	) tmp
WHERE
	goods_sn != '' AND goods_sn IS NOT NULL
GROUP BY
	date,
	goods_sn,
    glb_plf,
    mrlc;

--点击
INSERT OVERWRITE TABLE goods_date_cli_mrtemp
SELECT
	date,
	goods_sn,
	COUNT(*) click_num,
    glb_plf,
    mrlc
FROM(
	SELECT
		concat(year,month,day) date,
		get_json_object(a.glb_skuinfo,'$.sku') as goods_sn,
        glb_plf,
        get_json_object(glb_ubcta, '$.mrlc') as mrlc
	FROM
		stg.zf_pc_event_info a
	WHERE 
		glb_t = 'ic' AND concat(year,month,day) = '${ADD_TIME}'
         AND glb_skuinfo != '' AND glb_plf in ('pc','m') and geoip_country_name in ('Canada','France')
         and glb_x in ('addtobag','sku')
         and glb_b = 'a'
		 and glb_dc='1301'
	) tmp
GROUP BY
	date,
	goods_sn,
    glb_plf,
    mrlc;



--商品加购数
INSERT OVERWRITE TABLE goods_date_cart_mrtemp
SELECT
  date,
  goods_sn,
  count(*) AS cart_goods_number,
  glb_plf,
  mrlc
FROM
(
SELECT 
        concat(year,month,day) date,
		get_json_object(a.glb_skuinfo,'$.sku') as goods_sn,
        glb_plf,
        get_json_object(a.glb_ubcta, '$.fmd') as mrlc
        FROM
  stg.zf_pc_event_info a
WHERE
  concat(year,month,day) = '${ADD_TIME}'
  AND glb_t = 'ic'
  AND glb_x = 'ADT'
   AND glb_plf in ('pc','m') and geoip_country_name in ('Canada','France')
   and glb_dc='1301'
) tmp
group by
  date,
  goods_sn,
  glb_plf,
  mrlc
;


--商品收藏数	
INSERT OVERWRITE TABLE goods_date_coll_mrtemp
SELECT
	date,
  goods_sn,
  count(*) as collect_goods_number,
  glb_plf,
  mrlc
FROM
(
  SELECT 
        concat(year,month,day) date,
		get_json_object(a.glb_skuinfo,'$.sku') as goods_sn,
        glb_plf,
        get_json_object(a.glb_ubcta, '$.fmd') as mrlc
    FROM
  stg.zf_pc_event_info a
WHERE
  concat(year,month,day) = '${ADD_TIME}'
  AND glb_t = 'ic'
  AND glb_x = 'ADF'
   AND glb_plf in ('pc','m')
  AND glb_u <> ''
  and geoip_country_name in ('Canada','France')
  and glb_dc='1301'
) tmp
group by
  date,
  goods_sn,
  glb_plf,
  mrlc
;

--结果汇总

INSERT OVERWRITE TABLE lr_base_mrinfo partition(pdate = '${ADD_TIME}')
SELECT
    'pc' as plat,
	NVL(t1.goods_sn,''),
	NVL(t1.exp_num,0),
	NVL(t2.click_num,0),
	NVL(t4.collect_goods_number,0),
	NVL(t3.cart_goods_number,0),
	NVL(to_unix_timestamp(t1.date,"yyyyMMdd"),0),
	NVL(t1.date,'')
FROM
    (SELECT goods_sn,date,exp_num FROM
	goods_date_exp_mrtemp WHERE glb_plf='pc' and mrlc='T_2') t1
LEFT JOIN
    (SELECT goods_sn,date,click_num FROM
	goods_date_cli_mrtemp WHERE glb_plf='pc' and mrlc='T_2') t2
ON
	t1.date = t2.date AND t1.goods_sn = t2.goods_sn
LEFT JOIN
    (SELECT goods_sn,date,cart_goods_number FROM
	goods_date_cart_mrtemp WHERE glb_plf='pc' and mrlc='mr_T_2') t3
ON
    t1.date = t3.date AND t1.goods_sn = t3.goods_sn
LEFT JOIN
    (SELECT goods_sn,date,collect_goods_number FROM
	goods_date_coll_mrtemp WHERE glb_plf='pc' and mrlc='mr_T_2') t4
ON
    t1.date = t4.date AND t1.goods_sn = t4.goods_sn

UNION ALL

SELECT
    'm' as plat,
	NVL(t1.goods_sn,''),
	NVL(t1.exp_num,0),
	NVL(t2.click_num,0),
	NVL(t4.collect_goods_number,0),
	NVL(t3.cart_goods_number,0),
	NVL(to_unix_timestamp(t1.date,"yyyyMMdd"),0),
	NVL(t1.date,'')
FROM
    (SELECT goods_sn,date,exp_num FROM
	goods_date_exp_mrtemp WHERE glb_plf='m' and mrlc='T_1') t1
LEFT JOIN
    (SELECT goods_sn,date,click_num FROM
	goods_date_cli_mrtemp WHERE glb_plf='m' and mrlc='T_1') t2
ON
	t1.date = t2.date AND t1.goods_sn = t2.goods_sn
LEFT JOIN
    (SELECT goods_sn,date,cart_goods_number FROM
	goods_date_cart_mrtemp WHERE glb_plf='m' and mrlc='mr_T_1') t3
ON
    t1.date = t3.date AND t1.goods_sn = t3.goods_sn
LEFT JOIN
    (SELECT goods_sn,date,collect_goods_number FROM
	goods_date_coll_mrtemp WHERE glb_plf='m' and mrlc='mr_T_1') t4
ON
    t1.date = t4.date AND t1.goods_sn = t4.goods_sn
;
