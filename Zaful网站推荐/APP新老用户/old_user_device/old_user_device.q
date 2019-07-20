--@author ZhanRui
--@date 2018年11月13日 
--@desc  zaful老用户数据

SET mapred.job.name=old_user_device;
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


INSERT OVERWRITE TABLE dw_zaful_recommend.old_user_device
SELECT DISTINCT
	x.appsflyer_device_id
FROM
	(
		SELECT
			n.appsflyer_device_id
		FROM
			(
				SELECT
					user_id
				FROM
					ods.ods_m_zaful_eload_users
				WHERE
					dt = '${ADD_TIME}'
			) m
		JOIN dw_zaful_recommend.zaful_app_u_map n ON m.user_id = n.customer_user_id
		UNION ALL
			SELECT DISTINCT
				appsflyer_device_id
			FROM
				ods.ods_app_burial_log
			WHERE
				concat(year, month, day) BETWEEN '${ADD_TIME_W}'
			AND '${ADD_TIME}'
			AND site = 'zaful'
	) x
	where x.appsflyer_device_id is not null
;


INSERT overwrite TABLE dw_zaful_recommend.old_user_device_count PARTITION (dt = '${ADD_TIME}') SELECT
	count(*)
FROM
	dw_zaful_recommend.old_user_device

