--@author zhangyuchao
--@date 2019年03月27日 
--@desc GB老用户数据
--@wiki http://wiki.hqygou.com:8090/pages/viewpage.action?pageId=127404109   

SET mapred.job.name=gb_app_old_user_device_id_ninety_day;
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

INSERT OVERWRITE TABLE dw_gearbest_recommend.gb_app_old_user_device_id_ninety_day
-- 老用户的唯一标识
SELECT DISTINCT mn.appsflyer_device_id
from
(
	--1 --确定注册用户的唯一标识
		select 
			n.identify as appsflyer_device_id
		from
		    ods.ods_crm_userid_union_cookieid_mid_otn n 
		    where n.site='gearbest' 
		    and n.dt='${ADD_TIME}'
		    and n.user_id != ''
	--2 --确定90天内访问网站的非注册用户的唯一标识 
	UNION ALL
		SELECT DISTINCT
			appsflyer_device_id
		FROM
			ods.ods_app_burial_log
		WHERE
			concat(year, month, day) BETWEEN '${ADD_TIME_W}'
		AND '${ADD_TIME}'
		AND site = 'gearbest'
		--AND event_name='af_view_homepage' 
                AND event_name='af_page_view'
) mn


