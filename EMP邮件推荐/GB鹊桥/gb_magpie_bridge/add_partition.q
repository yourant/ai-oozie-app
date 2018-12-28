--@author ZhanRui
--@date 2018年11月06日 
--@desc  gb鹊桥项目订单数据导出准备

SET mapred.job.name=gb_order_sms_detail_exp;
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


Alter TABLE dw_gearbest_recommend.gb_magpie_bridge_result_gtq ADD IF NOT EXISTS PARTITION  (year=${YEAR},month=${MONTH},day=${DAY}) location  '/user/hive/warehouse/dw_gearbest_recommend.db/gb_magpie_bridge_result_gtq/year=${YEAR}/month=${MONTH}/day=${DAY}';


INSERT OVERWRITE TABLE dw_gearbest_recommend.gb_order_sms_detail_exp
SELECT
	a.order_sn, 
	a.goods_sn, 
	a.user_id, 
	b.created_time  update_time
FROM
	stg.gb_order_order_goods a
JOIN(
	SELECT
		order_sn,
		created_time
	FROM
		stg.gb_order_order_info
	) b
ON
	a.order_sn = b.order_sn
JOIN stg.t_sms_order_no c ON a.order_sn=c.order_no