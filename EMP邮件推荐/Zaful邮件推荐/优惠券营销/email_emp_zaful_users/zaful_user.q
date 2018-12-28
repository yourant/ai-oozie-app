--@author ZhanRui
--@date 2018年12月14日 
--@desc  zaful邮件推荐用户数据

SET mapred.job.name=email_emp_zaful_users;
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


--Zaful邮件用户
INSERT OVERWRITE TABLE dw_zaful_recommend.email_emp_zaful_users SELECT
	x.user_id,
	t.country_code
FROM
	(
		SELECT DISTINCT
			a.user_id
		FROM
			(
				SELECT
					user_id
				FROM
					ods.ods_m_zaful_eload_order_info
				WHERE
					dt = '${DATE}'
				AND FROM_UNIXTIME(pay_time + 8 * 3600, 'yyyyMMdd') BETWEEN '${DATE_W}'
				AND '${DATE}'
				AND order_status IN (1, 2, 3, 4, 6, 8, 15, 16, 20)
			) a
		JOIN (
			SELECT
				user_id
			FROM
				ods.ods_m_zaful_eload_users
			WHERE
				dt = '${DATE}'
			AND is_dingyue_success = 1
		) b ON a.user_id = b.user_id
	) x
LEFT JOIN dw_proj.zf_uc_map t ON x.user_id = t.user_id;