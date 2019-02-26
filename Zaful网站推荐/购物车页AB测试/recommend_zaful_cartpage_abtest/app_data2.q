-- APP订单数据(recommender_online, zaful_ordered_info_device_id)
set mapred.job.queue.name=root.ai.online; 
drop table if exists zaful_user_base_recommend.zaful_ordered_info_device_id;
create table zaful_user_base_recommend.zaful_ordered_info_device_id as 
SELECT
  a.appsflyer_device_id,
  g.goods_sn as sku,
  o.add_time
from
  stg.zaful_eload_order_info o
  join (
    select
      appsflyer_device_id,
      customer_user_id
    from
      stg.zaful_log_app_event
    where
      concat(year,month,day) = ${DATE}
  ) a on o.user_id = a.customer_user_id
  join stg.zaful_eload_order_goods g on o.order_id = g.order_id
group by
  a.appsflyer_device_id,
  g.goods_sn,
  o.add_time;