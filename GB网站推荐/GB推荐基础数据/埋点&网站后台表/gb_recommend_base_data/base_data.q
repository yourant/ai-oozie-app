--@author ZhanRui
--@date 2018年12月26日 
--@desc  gb邮件推荐基础数据-使用ODS表

SET mapred.job.name=ods_gb_recommend_base_data;
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


--埋点行为
INSERT overwrite TABLE dw_gearbest_recommend.email_gb_pc_event_info PARTITION (date = '${DATE}') SELECT
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
AND site = 'gearbest';



--订单信息
INSERT overwrite TABLE dw_gearbest_recommend.email_gb_goods_event_info PARTITION (date = '${DATE}') SELECT
	a.order_sn,
	a.order_status,
	b.goods_sn,
	a.user_id,
	a.created_time
FROM
	(
		SELECT
			order_sn,
			user_id,
			created_time,
			order_status
		FROM
			ods.ods_m_gearbest_gb_order_order_info
		WHERE
			dt = '${DATE}'
		AND from_unixtime(created_time + 8 * 3600, 'yyyyMMdd') = '${DATE}'
	) a
JOIN (
	SELECT
		order_sn,
		goods_sn
	FROM
		ods.ods_m_gearbest_gb_order_order_goods
	WHERE
		dt = '${DATE}'
) b ON a.order_sn = b.order_sn;

