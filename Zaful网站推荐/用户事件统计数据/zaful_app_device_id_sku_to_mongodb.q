
--@author wuchao
--@date 2018年10月31日 
--@desc  Zaful App新需求以appsflyer_device_id为主将数据导入mongodb


SET mapred.job.name=zaful_app_wuc_appsflyer_device_id_report_media_source;
set mapred.job.queue.name=root.ai.offline;
SET mapred.max.split.size=128000000;
SET mapred.min.split.size=64000000;
SET mapred.min.split.size.per.node=64000000;
SET mapred.min.split.size.per.rack=64000000;
SET hive.exec.reducers.bytes.per.reducer = 128000000;
SET hive.merge.mapfiles=true;
SET hive.merge.mapredfiles= true;
SET hive.input.format=org.apache.hadoop.hive.ql.io.CombineHiveInputFormat;
SET hive.merge.size.per.task=256000000;
SET hive.exec.parallel = true;


--输出结果表
CREATE TABLE IF NOT EXISTS dw_zaful_recommend.zaful_app_wuc_appsflyer_device_id_report_media_source(
user_cookie_id                STRING               COMMENT '用户访问cookie id',
user_id                       STRING               COMMENT '用户编号',
user_is_register              BOOLEAN               COMMENT '是否注册用户',
user_platform                 STRING               COMMENT '用户来源平台',
user_channel                  STRING               COMMENT '用户的来源渠道',
user_language                 STRING               COMMENT '语种',
user_device_brand             STRING               COMMENT '设备品牌',
user_country_code             STRING               COMMENT '国家简码',
user_device_model             STRING               COMMENT '设备型号',
user_register_time            BIGINT               COMMENT '注册时间',
user_register_from            STRING               COMMENT '注册平台',
user_register_ip              STRING               COMMENT '注册ip',
user_last_login               BIGINT               COMMENT '最后一次登录的时间',
user_last_ip                  STRING               COMMENT '最后一次登录的ip地址',
user_last_order_time          BIGINT               COMMENT '最后下单时间',
user_event_time               BIGINT               COMMENT '事件时间',
user_event_name               STRING               COMMENT '事件类型',
user_event_value_sku          STRING               COMMENT '事件内容SKU'
)
COMMENT 'Zaful App新需求以appsflyer_device_id为主将数据导入mongodb'
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\u0001' LINES TERMINATED BY '\n' STORED AS TEXTFILE
;
--输出结果表
CREATE TABLE IF NOT EXISTS dw_zaful_recommend.zaful_app_wuc_appsflyer_device_id_report_media_source_exp(
user_cookie_id                STRING               COMMENT '用户访问cookie id',
user_id                       STRING               COMMENT '用户编号',
user_is_register              STRING               COMMENT '是否注册用户',
user_platform                 STRING               COMMENT '用户来源平台',
user_channel                  STRING               COMMENT '用户的来源渠道',
user_language                 STRING               COMMENT '语种',
user_device_brand             STRING               COMMENT '设备品牌',
user_country_code             STRING               COMMENT '国家简码',
user_device_model             STRING               COMMENT '设备型号',
user_register_time            INT               COMMENT '注册时间',
user_register_from            STRING               COMMENT '注册平台',
user_register_ip              STRING               COMMENT '注册ip',
user_last_login               INT               COMMENT '最后一次登录的时间',
user_last_ip                  STRING               COMMENT '最后一次登录的ip地址',
user_last_order_time          INT               COMMENT '最后下单时间',
user_event_time               INT               COMMENT '事件时间',
user_event_name               STRING               COMMENT '事件类型',
user_event_value_sku          STRING               COMMENT '事件内容SKU'
)
COMMENT 'Zaful App新需求以appsflyer_device_id为主将数据导入mongodb'
PARTITIONED BY (add_time STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\u0001' LINES TERMINATED BY '\n' STORED AS TEXTFILE
;
--appsflyer_device_id去重后的表
CREATE TABLE IF NOT EXISTS dw_zaful_recommend.zaful_app_u_map_distinct_appsflyer_device_id_media_source(
appsflyer_device_id               STRING         COMMENT "",
customer_user_id                  STRING         COMMENT "",
num                               String         comment ""
)
COMMENT "appsflyer_device_id去重后的表"
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\u0001' LINES TERMINATED BY '\n' STORED AS TEXTFILE;

--更新appsflyer_device_id去重后的表
insert overwrite table dw_zaful_recommend.zaful_app_u_map_distinct_appsflyer_device_id_media_source
select * from (
  select 
    appsflyer_device_id, customer_user_id,
    row_number() over ( partition by appsflyer_device_id order by rand()  ) num 
  from  
        dw_zaful_recommend.zaful_app_u_map
) last 
  where last.num = 1 ;

  
--中间表
CREATE TABLE IF NOT EXISTS dw_zaful_recommend.zaful_app_wuc_appsflyer_device_id_tmp_media_source(
appsflyer_device_id               STRING         COMMENT "",
customer_user_id                  STRING         COMMENT "",
platform                          String         comment "",
af_channel                        STRING         COMMENT "",
language                          STRING         COMMENT "",
device_brand                      STRING         COMMENT "",
country_code                      STRING         COMMENT "",
device_model                      STRING         COMMENT "",
event_time                        BIGINT         COMMENT "",
event_name                        STRING         COMMENT "",
skus                              STRING         COMMENT ""
)
COMMENT "中间表"
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\u0001' LINES TERMINATED BY '\n' STORED AS TEXTFILE;
  
  
insert overwrite table dw_zaful_recommend.zaful_app_wuc_appsflyer_device_id_tmp_media_source
SELECT
m.appsflyer_device_id,
n.customer_user_id,
m.platform,
m.af_channel,
m.language,
m.device_brand,
m.country_code,
m.device_model,
m.event_time,
m.event_name,
m.skus
from 

(
SELECT
      appsflyer_device_id,
      platform,
      media_source as af_channel,
      language,
      device_brand,
      country_code,
      device_model,
      event_time / 1000 as event_time,
      event_name,
      case when event_name='af_search' and get_json_object(event_value, '$.af_search_page') in ('normal search','normal_search')
       then get_json_object(event_value, '$.af_content_type')
      else get_json_object(event_value, '$.af_content_id') end AS skus
FROM 
      ods.ods_app_burial_log
WHERE
      concat_ws('-', year, month, day) = '${ADD_TIME}'
      AND site='zaful'
      AND (event_name='af_create_order_success' OR event_name='af_search' OR event_name='af_add_to_bag' OR event_name='af_view_product' OR event_name='af_add_to_wishlist')
 ) m
 left join dw_zaful_recommend.zaful_app_u_map_distinct_appsflyer_device_id_media_source n ON m.appsflyer_device_id=n.appsflyer_device_id
  ;
  
 --sku数组表 join zaful_eload_users部分字段和最新订单时间
 insert overwrite table dw_zaful_recommend.zaful_app_wuc_appsflyer_device_id_report_media_source 
select 
m.appsflyer_device_id,
m.customer_user_id,
case when m.customer_user_id is null then false else true end as user_is_register,
m.platform,
m.af_channel,
m.language,
m.device_brand,
m.country_code,
m.device_model,
n.reg_time,
case when n.reg_from =1 then 'WEB' 
     when n.reg_from =2 then 'M' 
     when n.reg_from =3 then 'IOS' 
     when n.reg_from =4 then 'ANDROID' end as reg_from,
n.reg_ip,
n.last_login,
n.last_ip,
t.add_time,
m.event_time,
m.event_name,
m.skus

from dw_zaful_recommend.zaful_app_wuc_appsflyer_device_id_tmp_media_source m
  
left join stg.zaful_eload_users n on m.customer_user_id=n.user_id

left join 
(
  select 
  last.user_id as user_id,
  last.add_time as add_time
  from (
    select 
      user_id, add_time,
      row_number() over ( partition by user_id order by add_time desc  ) num 
    from  
        stg.zaful_eload_order_info
  ) last 
   where last.num = 1 
) t
on m.customer_user_id=t.user_id
  ;
  
--sku表
insert into table dw_zaful_recommend.zaful_app_wuc_appsflyer_device_id_report_media_source_exp PARTITION (add_time='${ADD_TIME}')
SELECT 
	    x.user_cookie_id,         
      coalesce(x.user_id,'') as user_id,                     
      case when x.user_is_register = true then 'true' else 'false' end as user_is_register,           
      coalesce(x.user_platform,'') as user_platform,        
      coalesce(x.user_channel,'') as user_channel,                
      coalesce(x.user_language,'') as user_language,               
      coalesce(x.user_device_brand,'') as user_device_brand,          
      coalesce(x.user_country_code,'') as user_country_code,          
      coalesce(x.user_device_model,'') as user_device_model,          
      coalesce(x.user_register_time,0) as user_register_time,       
      coalesce(x.user_register_from,'') as user_register_from,        
      coalesce(x.user_register_ip,'') as user_register_ip,         
      coalesce(x.user_last_login,0) as user_last_login,      
      coalesce(x.user_last_ip,'') as user_last_ip,           
      coalesce(x.user_last_order_time,0) as user_last_order_time,   
      coalesce(x.user_event_time,0) as user_event_time,       
      coalesce(x.user_event_name,'') as user_event_name,
      sku                    
    FROM 
    (
    select * from 
    dw_zaful_recommend.zaful_app_wuc_appsflyer_device_id_report_media_source
    where user_event_value_sku is not null and user_event_name !='af_search'
    ) x
    LATERAL VIEW explode(split(x.user_event_value_sku,',')) zqms as sku
union all
SELECT 
	user_cookie_id,         
      coalesce(user_id,'') as user_id,                     
      case when user_is_register = true then 'true' else 'false' end as user_is_register,           
      coalesce(user_platform,'') as user_platform,        
      coalesce(user_channel,'') as user_channel,                
      coalesce(user_language,'') as user_language,               
      coalesce(user_device_brand,'') as user_device_brand,          
      coalesce(user_country_code,'') as user_country_code,          
      coalesce(user_device_model,'') as user_device_model,          
      coalesce(user_register_time,0) as user_register_time,       
      coalesce(user_register_from,'') as user_register_from,        
      coalesce(user_register_ip,'') as user_register_ip,         
      coalesce(user_last_login,0) as user_last_login,      
      coalesce(user_last_ip,'') as user_last_ip,           
      coalesce(user_last_order_time,0) as user_last_order_time,   
      coalesce(user_event_time,0) as user_event_time,       
      coalesce(user_event_name,'') as user_event_name,
      '' as sku  
    FROM 
    dw_zaful_recommend.zaful_app_wuc_appsflyer_device_id_report_media_source
    where user_event_value_sku is null and user_event_name !='af_search'
union all
SELECT 
	user_cookie_id,         
      coalesce(user_id,'') as user_id,                     
      case when user_is_register = true then 'true' else 'false' end as user_is_register,           
      coalesce(user_platform,'') as user_platform,        
      coalesce(user_channel,'') as user_channel,                
      coalesce(user_language,'') as user_language,               
      coalesce(user_device_brand,'') as user_device_brand,          
      coalesce(user_country_code,'') as user_country_code,          
      coalesce(user_device_model,'') as user_device_model,          
      coalesce(user_register_time,0) as user_register_time,       
      coalesce(user_register_from,'') as user_register_from,        
      coalesce(user_register_ip,'') as user_register_ip,         
      coalesce(user_last_login,0) as user_last_login,      
      coalesce(user_last_ip,'') as user_last_ip,           
      coalesce(user_last_order_time,0) as user_last_order_time,   
      coalesce(user_event_time,0) as user_event_time,       
      coalesce(user_event_name,'') as user_event_name, 
      coalesce(user_event_value_sku,'') as sku
    FROM 
    dw_zaful_recommend.zaful_app_wuc_appsflyer_device_id_report_media_source
    where  user_event_name ='af_search'
  ;
  
  
  
  
  
  
  
  
  
  
  
  