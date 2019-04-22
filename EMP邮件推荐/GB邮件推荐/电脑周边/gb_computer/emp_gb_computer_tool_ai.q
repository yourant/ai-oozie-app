--@author DongTengfei
--@date 2019年4月04日 
--@desc  gb-computer

SET mapred.job.name=emp_gb_computer_Tool_ai;
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



INSERT OVERWRITE TABLE emp_data.emp_gb_computer_Tool_ai SELECT
    A.site_user_id,
    collect_set(A.region_code)[0],
    UNIX_TIMESTAMP()
from 
    ods.ods_m_emp_ems_edm_user_gearbest as A
left join 
    dm.dm_idp_fact as B 
on 
    A.site_user_id = B.user_id 
where 
    A.last_open > UNIX_TIMESTAMP()-86400*90 
and 
    A.site_user_id != 0 
and 
    B.level_id_1 in(11257,11347) 
and 
    B.user_id != '' 
and 
    B.dt > from_unixtime(UNIX_TIMESTAMP()-86400*90,'yyyyMMdd')
group by 
    A.site_user_id
;
