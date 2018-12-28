--@author ZhanRui
--@date 2018年11月08日 
--@desc  gb邮件推荐-每日营销邮件基础算法数据

SET mapred.job.name=gb_review_info;
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


--评论数据
insert overwrite table dw_gearbest_recommend.gb_review_info partition(pdate = 'none')
select a.goods_sku,a.user_id,a.is_favourable_comment,a.add_time,a.goods_spu,a.dt
from ods.ods_m_gearbest_review_service_gearbest_review a,ods.ods_m_gearbest_review_service_gearbest_review_info b 
where a.id=b.review_id 
and b.source=0
and a.is_del=0 
and a.`status`=1
and a.goods_sku!=''
and a.user_id!=0
and a.dt='${DATE}'
and a.user_id in (select site_user_id from ods.ods_m_emp_ems_edm_user_gearbest where dt='${DATE}' 
and last_open > UNIX_TIMESTAMP(
	DATE_SUB(
		FROM_UNIXTIME(
			UNIX_TIMESTAMP(),
			'yyyy-MM-dd'
		),
		15
	),
	'yyyy-MM-dd'
))
group by a.goods_sku,a.user_id,a.is_favourable_comment,a.add_time,a.goods_spu,a.dt;


--订单数据
insert overwrite table dw_gearbest_recommend.gb_order_goods_info
select a.order_sn,b.goods_sn,a.user_id,c.email,c.country,a.update_time,
case when a.order_status=0 and a.pay_status=0 then 'CO' 
when a.order_status=0 and a.pay_status=3 then 'PFM' 
when a.order_status=5 then 'REF' 
when (a.order_status=1 or a.order_status=2 or a.order_status=3) and a.pay_status=0 and a.distribute_status=0 then 'COO' 
else 'OTHER'
end as behavior 
from (select * from ods.ods_m_gearbest_gb_order_order_info where dt='${DATE}'
AND created_time< UNIX_TIMESTAMP()
AND created_time> UNIX_TIMESTAMP(
	DATE_SUB(
		FROM_UNIXTIME(
			UNIX_TIMESTAMP(),
			'yyyy-MM-dd'
		),
		90
	),
	'yyyy-MM-dd'
)) a join
(select * from  ods.ods_m_gearbest_gb_order_order_goods where dt='${DATE}') b
on a.order_sn = b.order_sn
join (select * from ods.ods_m_emp_ems_edm_user_gearbest where dt='${DATE}' 
and last_open > UNIX_TIMESTAMP(
	DATE_SUB(
		FROM_UNIXTIME(
			UNIX_TIMESTAMP(),
			'yyyy-MM-dd'
		),
		15
	),
	'yyyy-MM-dd'
)) xx on xx.site_user_id=a.user_id
left join (select e.email,e.user_id,d.country from
( select * from ods.ods_m_gearbest_gb_member_mem_reg_log where dt='${DATE}') d 
join ( select * from ods.ods_m_gearbest_gb_member_user_base where dt='${DATE}') e on d.email=e.email ) c
on a.user_id=c.user_id;











