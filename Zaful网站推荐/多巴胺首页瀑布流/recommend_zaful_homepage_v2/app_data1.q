-- APP点击数据(recommender_online, zaful_log_app_event_detail_exp)
set mapred.job.queue.name=root.ai.online; 
drop table if exists zaful_user_base_recommend.zaful_log_app_event_detail_exp;
create table zaful_user_base_recommend.zaful_log_app_event_detail_exp as 
select
  appsflyer_device_id,
  platform,
  event_name,
  get_json_object(event_value, '$.af_content_id') as sku,
  event_time
from
  stg.zaful_log_app_event
where
  concat(year,month,day) = ${ADD_TIME}
  and get_json_object(event_value, '$.af_content_id') != '';