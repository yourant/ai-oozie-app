--@author wuchao
--@date 2018年12月17日 
--@desc  rg邮件推荐基础数据-使用ODS表

SET mapred.job.name=ods_rosegal_recommend_base_data;
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


CREATE TABLE IF NOT EXISTS dw_rg_recommend.email_rosegal_pc_event_info(
goods_sn                  STRING         COMMENT "sku",
glb_oi                    STRING         COMMENT "用户会话id",
glb_u                     bigint         COMMENT "user_id",
glb_od                    STRING         COMMENT "cookie id",
glb_w                     bigint         COMMENT "页面停留时间",
glb_x                     STRING         COMMENT "子事件详情",
glb_tm                    bigint         COMMENT "当前时间戳",
year                      STRING         COMMENT "年",
month                     STRING         COMMENT "月",
day                       STRING         COMMENT "日",
add_time                  bigint         COMMENT "时间"
)
COMMENT 'rosegal pc邮件数据报表'
PARTITIONED BY (date STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\u0001' LINES TERMINATED BY '\n' STORED AS TEXTFILE
;



--埋点行为
INSERT overwrite TABLE dw_rg_recommend.email_rosegal_pc_event_info PARTITION (date = '${DATE}') SELECT
	get_json_object (skuinfo, '$.sku') AS goods_sn,
	session_id AS glb_oi,
	CASE
WHEN user_id = '' THEN
	0
ELSE
	user_id
END AS glb_u,
 cookie_id AS glb_od,
 page_stay_time AS glb_w,
 sub_event_info AS glb_x,
 time_stamp AS glb_tm,
 YEAR,
 MONTH,
 DAY,
 unix_timestamp(
	concat(YEAR, MONTH, DAY),
	'yyyyMMdd'
) AS add_time
FROM
	ods.ods_pc_burial_log
WHERE
	concat(YEAR, MONTH, DAY) = '${DATE}'
AND behaviour_type = 'ic'
AND skuinfo <> ''
AND site = 'rosegal';


CREATE TABLE IF NOT EXISTS dw_rg_recommend.email_rosegal_goods_event_info(
order_id                  STRING         COMMENT "订单id",
order_status                    smallint         COMMENT "订单状态",
goods_sn                     string         COMMENT "sku",
user_id                    bigint         COMMENT "user_id",
add_time                     bigint         COMMENT "时间"
)
COMMENT 'rosegal pc邮件数据报表'
PARTITIONED BY (date STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\u0001' LINES TERMINATED BY '\n' STORED AS TEXTFILE
;


--订单信息
INSERT overwrite TABLE dw_rg_recommend.email_rosegal_goods_event_info PARTITION (date = '${DATE}') 
SELECT
	a.order_id,
	a.order_status,
	b.goods_sn,
	a.user_id,
	a.add_time
FROM
	(
		SELECT
			  order_id,
			user_id,
			add_time,
			order_status
		FROM
			ods.ods_m_rosegal_eload_order_info
		WHERE
			dt = '${DATE}'
		AND from_unixtime(add_time + 8 * 3600, 'yyyyMMdd') = '${DATE}'
		group by order_id,user_id,order_status,add_time
	) a
JOIN (
	SELECT
		 order_id,
		goods_sn
	FROM
		ods.ods_m_rosegal_eload_order_goods
	WHERE
		dt = '${DATE}'
	AND from_unixtime(addtime + 8 * 3600, 'yyyyMMdd') = '${DATE}'
	group by order_id,goods_sn
) b ON a.order_id = b.order_id;