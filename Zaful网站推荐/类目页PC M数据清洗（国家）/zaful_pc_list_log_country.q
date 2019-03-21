--@author zhanrui
--@date 2019年03月01日 
--@desc  zaful类目页数据清洗

SET mapred.job.name='zaful_pc_list_log_country&order_info';
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
set hive.auto.convert.join=false;


--最终结果数据汇总
CREATE TABLE IF NOT EXISTS dw_zaful_recommend.zaful_pc_list_log_country(
    glb_tm           string   COMMENT '时间戳', 
    page             string   COMMENT '页面索引', 
  	sku              string   COMMENT '商品SKU',
    price            string   COMMENT '当时的商品价格',
    discount_mark    string   COMMENT '当时的折扣标',  
    platform         string   COMMENT '分端(pc、m)',
    glb_t            string   COMMENT '事件类型(ie/ic)',
    glb_x            string   COMMENT '事件名称(sku/addtobag/adt)',
    rank             string   COMMENT 'rank',
    glb_od           string   COMMENT '用户的cookieid',
    country_code     string   COMMENT '国家简码',
    glb_dc           string   COMMENT '国家站'
	)
COMMENT "类目页埋点日志清洗"
PARTITIONED BY (`date` string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\u0001' 
LINES TERMINATED BY '\n' 
STORED AS TEXTFILE;


--最终结果数据汇总(数据拼接后)
CREATE TABLE IF NOT EXISTS dw_zaful_recommend.zaful_pc_list_log_country_new(
    platform         string   COMMENT '平台', 
    glb_t            string   COMMENT '事件类型(ie/ic)',
    glb_od           string   COMMENT '用户的cookieid',
    sku_info         string   COMMENT '相关信息拼接',
    country_code     string   COMMENT '国家简码',
    glb_dc           string   COMMENT '国家站'
	)
COMMENT "类目页埋点日志清洗2"
PARTITIONED BY (`date` string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\u0001' 
LINES TERMINATED BY '\n' 
STORED AS TEXTFILE;


--埋点日志中间数据
CREATE TABLE IF NOT EXISTS dw_zaful_recommend.zaful_pc_list_log_country_tmp(
    glb_tm           string   COMMENT '时间戳', 
    page             string   COMMENT '页面索引', 
  	sku              string   COMMENT '商品SKU',
    price            string   COMMENT '当时的商品价格',
    discount_mark    string   COMMENT '当时的折扣标',  
    platform         string   COMMENT '分端(pc、m)',
    glb_t            string   COMMENT '事件类型(ie/ic)',
    glb_x            string   COMMENT '事件名称(sku/addtobag/adt)',
    rank             string   COMMENT '商品下单数量',
    glb_od           string   COMMENT '用户的cookieid',
    country_code     string   COMMENT '国家简码',
    glb_dc           string   COMMENT '国家站'
	)
COMMENT "类目页埋点日志清洗中间结果"
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\u0001'
LINES TERMINATED BY '\n'
STORED AS TEXTFILE;


--曝光数据
INSERT OVERWRITE TABLE dw_zaful_recommend.zaful_pc_list_log_country_tmp
SELECT
	time_stamp as glb_tm,
	page,
	goods_sn as sku,
  price,
  discount_mark,
  platform,
  behaviour_type as glb_t,
  sub_event_info as glb_x,
  rank,
  cookie_id as glb_od,
  country_code,
  country_number as glb_dc
FROM(
	SELECT
		get_json_object(a.sub_event_field,'$.sku') as goods_sn,
    get_json_object(a.sub_event_field,'$.price') as price,
    get_json_object(a.sub_event_field,'$.discount_mark') as discount_mark,
    get_json_object(a.sub_event_field,'$.rank') as rank,
    b.platform,
    get_json_object(b.page_info,'$.page') as page,
    b.time_stamp,
    b.behaviour_type,
    b.sub_event_info,
    b.cookie_id,
    b.country_code,
    b.country_number
	FROM(
		SELECT
			log_id,
			sub_event_field
		FROM
			ods.ods_pc_burial_log_ubcta
		WHERE
			concat(year,month,day) = '${ADD_TIME}'
            and site='zaful'
		) a
	JOIN(
		SELECT
			log_id,
      platform,
      page_info,
      time_stamp,
      behaviour_type,
      sub_event_info,
      cookie_id,
      country_code,
      country_number
		FROM
			ods.ods_pc_burial_log
		WHERE
		    concat(year,month,day) = '${ADD_TIME}'
            and site='zaful'
            AND behaviour_type = 'ie' AND  sub_event_field != '' AND platform in ('pc','m')
            and page_module = 'mp'
			and page_main_type='b'
            and page_code rlike 'category'
            --and country_number = 'ZF'
		) b
	ON
		a.log_id = b.log_id
	) tmp
WHERE
	goods_sn != '' AND goods_sn IS NOT NULL
;



--点击数据
INSERT INTO TABLE dw_zaful_recommend.zaful_pc_list_log_country_tmp
SELECT
	time_stamp as glb_tm,
	page,
	goods_sn as sku,
  price,
  discount_mark,
  platform,
  behaviour_type as glb_t,
  sub_event_info as glb_x,
  rank,
  cookie_id as glb_od,
  country_code,
  country_number as glb_dc
FROM(
	SELECT
		get_json_object(a.skuinfo,'$.sku') as goods_sn,
    get_json_object(a.sub_event_field,'$.price') as price,
    get_json_object(a.sub_event_field,'$.discount_mark') as discount_mark,
    get_json_object(a.sub_event_field,'$.rank') as rank,
    get_json_object(a.page_info,'$.page') as page,
    platform,
    time_stamp,
    behaviour_type,
    sub_event_info,
    cookie_id,
    country_code,
    country_number
	FROM
		ods.ods_pc_burial_log a
	WHERE 
		concat(year,month,day) = '${ADD_TIME}'
        and site='zaful'
        AND behaviour_type = 'ic' AND  sub_event_field != '' AND platform in ('pc','m')
        and page_module = 'mp' and sub_event_info in ('addtobag','sku')
		and page_main_type='b'
        and page_code rlike 'category'
        --and country_number = 'ZF'
	) tmp
;


--加购、收藏数据
INSERT INTO TABLE dw_zaful_recommend.zaful_pc_list_log_country_tmp
SELECT
	time_stamp as glb_tm,
	page,
	goods_sn as sku,
  price,
  discount_mark,
  platform,
  behaviour_type as glb_t,
  sub_event_info as glb_x,
  rank,
  cookie_id as glb_od,
  country_code,
  country_number as glb_dc
FROM(
	SELECT
		get_json_object(a.skuinfo,'$.sku') as goods_sn,
    get_json_object(a.sub_event_field,'$.price') as price,
    get_json_object(a.sub_event_field,'$.discount_mark') as discount_mark,
    get_json_object(a.sub_event_field,'$.rank') as rank,
    get_json_object(a.page_info,'$.page') as page,
    platform,
    time_stamp,
    behaviour_type,
    sub_event_info,
    cookie_id,
    country_code,
    country_number
	FROM
		ods.ods_pc_burial_log a
	WHERE 
		concat(year,month,day) = '${ADD_TIME}'
        and site='zaful'
        AND behaviour_type = 'ic' AND  sub_event_field != '' AND platform in ('pc','m')
        and sub_event_info in ('ADT','ADF')
        and get_json_object(a.sub_event_field,'$.fmd')='mp'
        and get_json_object(a.sub_event_field,'$.p') rlike 'category'
        --and country_number = 'ZF'        
	) tmp
;


--中间结果写入最终分区结果表
INSERT OVERWRITE TABLE dw_zaful_recommend.zaful_pc_list_log_country  partition (`date` = '${ADD_TIME}')
SELECT * FROM dw_zaful_recommend.zaful_pc_list_log_country_tmp;


--最终结果拼接
INSERT OVERWRITE TABLE dw_zaful_recommend.zaful_pc_list_log_country_new  partition (`date` = '${ADD_TIME}')
SELECT
	x.platform,
	x.glb_t,
	x.glb_od,
	concat_ws(
		',',
		collect_set (x.sku_info)
	),
  x.country_code,
  x.glb_dc
FROM
	(
		SELECT
			platform,
			glb_t,
			glb_od,
			concat_ws(
				'#',
				sku,
				glb_tm,
				rank,
				page,
				price,
				discount_mark,
				glb_x
			) AS sku_info,
      country_code,
      glb_dc
		FROM
			dw_zaful_recommend.zaful_pc_list_log_country_tmp
	) x
GROUP BY
	x.platform,
	x.glb_t,
	x.glb_od,
  x.country_code,
  x.glb_dc;

--订单表结果数据
CREATE TABLE IF NOT EXISTS dw_zaful_recommend.zaful_order_info_new(
    user_id           string   COMMENT '用户ID', 
    sku               string   COMMENT 'sku', 
  	time              string   COMMENT '下单时间',
    platform          string   COMMENT '平台',
    price             string   COMMENT '商品价格',
    order_status      string   COMMENT '订单状态',
    cookieid          string   COMMENT 'cookieid'
	)
COMMENT "订单表中间结果"
PARTITIONED BY (`date` string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\u0001'
LINES TERMINATED BY '\n'
STORED AS TEXTFILE;


--订单表中间数据
CREATE TABLE IF NOT EXISTS dw_zaful_recommend.zaful_order_info_new_tmp(
    user_id           string   COMMENT '用户ID', 
    sku               string   COMMENT 'sku', 
  	time              string   COMMENT '下单时间',
    platform          string   COMMENT '平台',
    price             string   COMMENT '商品价格',
    order_status      string   COMMENT '订单状态'
	)
COMMENT "订单表中间结果"
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\u0001'
LINES TERMINATED BY '\n'
STORED AS TEXTFILE;


--订单表中间数据
INSERT OVERWRITE TABLE dw_zaful_recommend.zaful_order_info_new_tmp 
SELECT
  x.user_id,
  p.goods_sn AS sku,
  x.add_time AS time,
  x.platform,
  p.goods_price AS price,
  x.order_status
FROM
(
  SELECT
    order_id,
    user_id,
    add_time,
    order_status,
    CASE  WHEN a.order_sn LIKE 'UU1%' OR   a.order_sn   LIKE 'U1%'  THEN 'pc'
          WHEN a.order_sn LIKE 'UL%'  OR   a.order_sn   LIKE 'UM%'  THEN 'm'
          WHEN a.order_sn LIKE 'UA%'  OR   a.order_sn   LIKE 'UUA%' THEN 'ios'
          WHEN a.order_sn LIKE 'UB%'  OR   a.order_sn   LIKE 'UUB%' THEN 'android'
          END AS platform
  FROM
    ods.ods_m_zaful_eload_order_info a
  WHERE
    dt = '${ADD_TIME}' 
    and from_unixtime(add_time + 8*3600, 'yyyyMMdd') = '${ADD_TIME}'
) x
JOIN   (
  SELECT 
    goods_sn,
    order_id,
    goods_price
  from
    ods.ods_m_zaful_eload_order_goods 
  WHERE
    dt = '${ADD_TIME}' 
) p
ON x.order_id = p.order_id
;


--关联cookie id 写入最终分区结果表
INSERT OVERWRITE TABLE dw_zaful_recommend.zaful_order_info_new  partition (`date` = '${ADD_TIME}')
SELECT
	x.user_id,
	x.sku,
	x.time,
	x.platform,
  x.price,
	x.order_status,
	x.glb_od
FROM
	(
		SELECT
			m.user_id,
			m.sku,
			m.time,
			m.platform,
			m.order_status,
      m.price,
			n.glb_od,
			ROW_NUMBER () OVER (
				PARTITION BY m.user_id,
				m.sku,
				m.time,
				m.platform,
				m.order_status
			ORDER BY
				rand()
			) AS flag
		FROM
			dw_zaful_recommend.zaful_order_info_new_tmp m
		JOIN dw_zaful_recommend.zaful_od_u_map n ON m.user_id = n.glb_u
	) x
WHERE
x.flag = 1
;
