-- APP点击数据(recommender_online, zaful_eload_cart_detail_exp)
set mapred.job.queue.name=root.ai.online; 
drop table if exists dw_zaful_recommend.zaful_eload_cart_detail_exp;
create table if not exists dw_zaful_recommend.zaful_eload_cart_detail_exp as
select
  session_id,
  goods_sn,
  addtime
from
  stg.zaful_eload_cart
where
  from_unixtime(addtime, 'yyyyMMdd') = ${DATE};