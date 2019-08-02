
SET mapred.job.name=Hawk_eye_data_monitoring_reference;
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

drop table  ai_data_analysis.Data_check;

create table ai_data_analysis.Data_check
as
select site, platform ,avg(coun) as rowcount, current_timestamp as InsertDT

from 
(
SELECT 
site
,concat(year,'-',month,'-',day)
,count(*) coun
,platform
from
ods.ods_pc_burial_log  
where concat(year,'-',month,'-',day)>date_add(CURRENT_DATE,-7)
and lower(site) in ('zaful','gearbest')
and lower(platform) in ('pc','m')
group by site,year,month,day,platform
 
 union all
 
 SELECT 
 site
,concat(year,'-',month,'-',day)
,count(*) coun
,platform
from
ods.ods_app_burial_log  
where concat(year,'-',month,'-',day)>date_add(CURRENT_DATE,-7)
and lower(site) in ('zaful','gearbest')
and lower(platform) in ('ios','android')
 group by site,year,month,day,platform
 
) A
group by site, platform
;



drop table  ai_data_analysis.Data_check_orderInfo;

create table ai_data_analysis.Data_check_orderInfo
as

select site ,avg(coun) as rowcount,current_timestamp as InsertDT
from 
(

select 'zaful' as site,dt,count(*) as coun from ods.ods_m_zaful_eload_order_info 
where dt>regexp_replace(DATE_ADD(current_date,-7),'-','')
group by dt

union all

 
select 'gearbest' as site ,dt,count(*) as coun from ods.ods_m_gearbest_gb_order_order_info

where dt>regexp_replace(DATE_ADD(current_date,-7),'-','')
group by dt
 
) A
group by site
;







drop table  ai_data_analysis.Data_check_ordergoods;

create table ai_data_analysis.Data_check_ordergoods
as

select site ,avg(coun) as rowcount,current_timestamp as InsertDT
from 
(

select 'zaful' as site,dt,count(*) as coun from ods.ods_m_zaful_eload_order_goods 
where dt>regexp_replace(DATE_ADD(current_date,-7),'-','')
group by dt

union all

 
select 'gearbest' as site ,dt,count(*) as coun from ods.ods_m_gearbest_gb_order_order_goods

where dt>regexp_replace(DATE_ADD(current_date,-7),'-','')
group by dt
 
) A
group by site
;

drop table ai_data_analysis.data_check_update;

create table ai_data_analysis.data_check_update
as


select
shortname,
fullname,
avg(coun) as rowcount,
date_add(CURRENT_DATE,-1) as insertdt
from
(
select
"gbbgg" as shortname,
"ods.ods_m_gearbest_base_goods_goods" as fullname, 
dt,
count(*)  as coun
from ods.ods_m_gearbest_base_goods_goods 
where dt>date_add(CURRENT_DATE,-7)
group by dt


union all


select
"gbgpf" as shortname,
"ods.ods_m_gearbest_goods_price_factor" as fullname, 
dt,
count(*)  as coun
from ods.ods_m_gearbest_goods_price_factor 
where dt>date_add(CURRENT_DATE,-7)
group by dt



union all



select
"gbbgngl" as shortname,
"ods.ods_m_gearbest_base_goods_new_goods_label" as fullname, 
dt,
count(*)  as coun
from ods.ods_m_gearbest_base_goods_new_goods_label 
where dt>date_add(CURRENT_DATE,-7)
group by dt



union all



select
"zaful_rszri" as shortname,
"ods.ods_m_zaful_review_service_zaful_review_info" as fullname, 
dt,
count(*)  as coun
from ods.ods_m_zaful_review_service_zaful_review_info 
where dt>date_add(CURRENT_DATE,-7)
group by dt



union all



select
"zaful_rszr" as shortname,
"ods.ods_m_zaful_review_service_zaful_review" as fullname, 
dt,
count(*)  as coun
from ods.ods_m_zaful_review_service_zaful_review 
where dt>date_add(CURRENT_DATE,-7)
group by dt



)a
group by shortname,
fullname



union all



select
shortname,
fullname,
avg(coun) as rowcount,
date_add(CURRENT_DATE,-1) as insertdt
from
(
select
"zaful_eoi" as shortname,
"stg.zaful_eload_order_info" as fullname, 
update_time,
count(*)  as coun
from stg.zaful_eload_order_info 
where update_time>date_add(CURRENT_DATE,-7)
group by update_time



union all



select
"zaful_eog" as shortname,
"stg.zaful_eload_order_goods" as fullname, 
update_time,
count(*)  as coun
from stg.zaful_eload_order_goods 
where update_time>date_add(CURRENT_DATE,-7)
group by update_time



union all



select
"zaful_eg" as shortname,
"stg.zaful_eload_goods" as fullname, 
update_time,
count(*)  as coun
from stg.zaful_eload_goods 
where update_time>date_add(CURRENT_DATE,-7)
group by update_time



)b
group by shortname,
fullname



union all



select
shortname,
fullname,
avg(coun) as rowcount,
date_add(CURRENT_DATE,-1) as insertdt
from
(


select
"zaful_ege" as shortname,
"stg.zaful_eload_goods_extend" as fullname, 
wh_update_time,
count(*)  as coun
from stg.zaful_eload_goods_extend
where wh_update_time>date_add(CURRENT_DATE,-7)
group by wh_update_time



union all



select
"zaful_eu" as shortname,
"stg.zaful_eload_users" as fullname, 
wh_update_time,
count(*)  as coun
from stg.zaful_eload_users
where wh_update_time>date_add(CURRENT_DATE,-7)
group by wh_update_time


)c
group by shortname,
fullname
;

drop table ai_data_analysis.data_check_update;

create table ai_data_analysis.data_check_update
as

select
shortname,
fullname,
avg(coun) as rowcount,
date_add(CURRENT_DATE,-1) as insertdt
from
(
select
"gbbgg" as shortname,
"ods.ods_m_gearbest_base_goods_goods" as fullname, 
dt,
count(*)  as coun
from ods.ods_m_gearbest_base_goods_goods 
where dt>regexp_replace(DATE_ADD(current_date,-7),'-','')
group by dt


union all


select
"gbgpf" as shortname,
"ods.ods_m_gearbest_goods_price_factor" as fullname, 
dt,
count(*)  as coun
from ods.ods_m_gearbest_goods_price_factor 
where dt>regexp_replace(DATE_ADD(current_date,-7),'-','')
group by dt


union all


select
"gbbgngl" as shortname,
"ods.ods_m_gearbest_base_goods_new_goods_label" as fullname, 
dt,
count(*)  as coun
from ods.ods_m_gearbest_base_goods_new_goods_label 
where dt>regexp_replace(DATE_ADD(current_date,-7),'-','')
group by dt


union all


select
"zaful_rszri" as shortname,
"ods.ods_m_zaful_review_service_zaful_review_info" as fullname, 
dt,
count(*)  as coun
from ods.ods_m_zaful_review_service_zaful_review_info 
where dt>regexp_replace(DATE_ADD(current_date,-7),'-','')
group by dt


union all


select
"zaful_rszr" as shortname,
"ods.ods_m_zaful_review_service_zaful_review" as fullname, 
dt,
count(*)  as coun
from ods.ods_m_zaful_review_service_zaful_review 
where dt>regexp_replace(DATE_ADD(current_date,-7),'-','')
group by dt


)a
group by shortname,
fullname


union all

  
select
shortname,
fullname,
avg(coun) as rowcount,
date_add(CURRENT_DATE,-1) as insertdt
from
(
select
"zaful_eoi" as shortname,
"stg.zaful_eload_order_info" as fullname, 
to_date(update_time) as update_time,
count(*)  as coun
from stg.zaful_eload_order_info 
where to_date(update_time)>date_add(CURRENT_DATE,-7)

group by to_date(update_time)


union all


select
"zaful_eog" as shortname,
"stg.zaful_eload_order_goods" as fullname, 
to_date(update_time) as update_time,
count(*)  as coun
from stg.zaful_eload_order_goods 
where to_date(update_time)>date_add(CURRENT_DATE,-7)

group by to_date(update_time)


union all


select
"zaful_eg" as shortname,
"stg.zaful_eload_goods" as fullname, 
to_date(update_time) as update_time,
count(*)  as coun
from stg.zaful_eload_goods 
where to_date(update_time)>date_add(CURRENT_DATE,-7)
group by to_date(update_time)


)b
group by shortname,
fullname


union all


select
shortname,
fullname,
avg(coun) as rowcount,
date_add(CURRENT_DATE,-1) as insertdt
from
(

select
"zaful_ege" as shortname,
"stg.zaful_eload_goods_extend" as fullname, 
to_date(wh_update_time) as wh_update_time,
count(*)  as coun
from stg.zaful_eload_goods_extend
where to_date(wh_update_time)>date_add(CURRENT_DATE,-7)
group by to_date(wh_update_time)


union all


select
"zaful_eu" as shortname,
"stg.zaful_eload_users" as fullname, 
to_date(wh_update_time) as wh_update_time,
count(*)  as coun
from stg.zaful_eload_users
where to_date(wh_update_time)>date_add(CURRENT_DATE,-7)
group by to_date(wh_update_time)

)c
group by shortname,
fullname
;

CREATE TABLE IF NOT EXISTS dw_proj.data_monitoring_reference_confirm(
	dd					STRING		   COMMENT ''
)
PARTITIONED BY (date String)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\u0001' LINES TERMINATED BY '\n' STORED AS TEXTFILE;


insert OVERWRITE table  dw_proj.data_monitoring_reference_confirm PARTITION (date = '${add_time}')
select
'${add_time}'
;



