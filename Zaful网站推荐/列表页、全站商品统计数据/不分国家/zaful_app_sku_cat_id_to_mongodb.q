
--@author wuchao
--@date 2018年12月12日 
--@desc  Zaful App新需求以sku,cat_id导入mongodb


SET mapred.job.name=zaful_app_sku_cat_id_mongodb;
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
  create table if not exists dw_proj.app_zaful_sku_cat_id_country_category_wuc_wishlist_temp(
 sku                        STRING    comment '商品id',
 af_impression              INT       comment '曝光量',
 af_view_product            INT       comment '点击量',
 af_add_to_wishlist         	INT   COMMENT '收藏量',
 af_add_to_bag_user              INT       comment '加购量按事件',
 af_add_to_bag_num              INT       comment '加购量按数量',
 af_create_order_success_user   int        comment '创建订单按事件',
 af_create_order_success_num   int        comment '创建订单按数量',
 af_purchase_user   int        comment '支付订单按事件',
 af_purchase_num   int        comment '支付订单按数量',
 gmv_user   decimal(10,4)        comment '',
 gmv_num   decimal(10,4)        comment '',
 sale_value_user   decimal(10,4)        comment '',
 sale_value_num   decimal(10,4)        comment '',
 event_time                 INT       comment '时间戳',
 date                       int       comment '时间',       
 platform                   STRING    comment '平台',
 cat_id                     int       comment '分类id',
 country_code               STRING    comment '国家代码'
 )
 PARTITIONED BY (add_time STRING)
 ROW FORMAT DELIMITED FIELDS TERMINATED BY '\u0001' LINES TERMINATED BY '\n' STORED AS TEXTFILE
 ;
 
 --执行查询，将结果写入hive表分区
insert overwrite table dw_proj.app_zaful_sku_cat_id_country_category_wuc_wishlist_temp PARTITION (add_time='${ADD_TIME}')
select
c.sku,
c.af_impression,
c.af_view_product,
c.af_add_to_wishlist,
c.af_add_to_bag,
c.af_add_to_bag_num,
c.af_create_order_success_user,
c.af_create_order_success_num,
c.af_purchase_user,
c.af_purchase_num,
c.gmv_user,
c.gmv_num,
c.sale_value_user,
c.sale_value_num,
c.add_times,
c.date,
c.platform,
k.cat_id,
c.country_code
from 
(
		SELECT
		coalesce(j.sku,p.sku,q.sku,'') as sku,
		j.af_impression as af_impression,
		j.af_view_product as af_view_product,
		j.af_add_to_wishlist as af_add_to_wishlist,
		j.af_add_to_bag_user as af_add_to_bag,
		j.af_add_to_bag_num as af_add_to_bag_num,
		p.create_order_count as af_create_order_success_user,
		p.create_order_sum as af_create_order_success_num,
		q.purchase_count as af_purchase_user,
		q.purchase_sum as af_purchase_num,
		p.gmv_user as gmv_user,
		p.gmv_num as gmv_num,
		q.sale_value_user as sale_value_user,
		q.sale_value_num as sale_value_num,
		UNIX_TIMESTAMP('${ADD_TIME}','yyyyMMdd') as add_times,
		from_unixtime(UNIX_TIMESTAMP('${ADD_TIME}','yyyyMMdd'),'yyyyMMdd') as date,
		 coalesce(j.platform,p.platform,q.platform,'') as platform,
		k.cat_id as cat_id,
		coalesce(j.country_code,p.country_code,q.country_code,'') as country_code
		FROM 

		(
		SELECT 
		a.sku as sku,a.platform as platform,a.country_code as country_code,
		count(case when a.event_name='af_view_product' and get_json_object(event_value, '$.af_changed_size_or_color') = 0 then 1 else null end) as af_view_product,
		count(case when a.event_name='af_impression' then 1 else null end) as af_impression,
		count(case when a.event_name='af_add_to_wishlist' then 1 else null end) as af_add_to_wishlist,
		count(case when a.event_name='af_add_to_bag'  then 1 else null end) as af_add_to_bag_user,
		sum(case when a.event_name='af_add_to_bag'  then a.num  else 0 end) as af_add_to_bag_num,
		count(case when a.event_name='af_create_order_success'  then 1 else null end) as af_create_order_success_user,
		sum(case when a.event_name='af_create_order_success'  then a.num  else 0 end) as af_create_order_success_num,
		count(case when a.event_name='af_purchase'  then 1 else null end) as af_purchase_user,
		sum(case when a.event_name='af_purchase'  then a.num  else 0 end) as af_purchase_num

		FROM
			(
				select x.sku,y.num as num,x.event_name,x.event_value,x.event_time,x.platform,x.country_code from 

					(SELECT 
						sku,
						n.skus as skus,
						n.af_quantity as af_quantity,
						event_name,
							event_value,
						event_time,
						platform,
						country_code
							FROM (SELECT
								m.event_name as event_name,
											m.event_value as event_value,
								m.event_time as event_time,
								m.af_content_id as skus,
								m.af_quantity as af_quantity,
								m.platform as platform,
											m.country_code as country_code
								FROM
								(
									SELECT
									event_name,
													event_value,
									event_time,
									case when substr(get_json_object(event_value, '$.af_content_id'),-1,1)=',' then
									substr(get_json_object(event_value, '$.af_content_id'),0,length(get_json_object(event_value, '$.af_content_id'))-1) 
									else get_json_object(event_value, '$.af_content_id') end as af_content_id,
									case when substr(get_json_object(event_value, '$.af_quantity'),-1,1)=',' then
									substr(get_json_object(event_value, '$.af_quantity'),0,length(get_json_object(event_value, '$.af_quantity'))-1) 
									else get_json_object(event_value, '$.af_quantity') end as af_quantity,
									platform,
													country_code
									FROM ods.ods_app_burial_log
									WHERE
									concat(year,month,day) = '${ADD_TIME}'
									and site='zaful'
									and get_json_object(event_value,'$.af_content_id') is not null
									and get_json_object(event_value,'$.af_content_id') != ''
									AND  get_json_object(event_value, '$.af_inner_mediasource') like 'category_%'
									--and get_json_object(event_value, '$.af_inner_mediasource') !='category_(null)'
									AND (event_name='af_add_to_wishlist' OR  event_name='af_add_to_bag' OR event_name='af_view_product'  OR event_name='af_impression' OR event_name='af_create_order_success' OR event_name='af_purchase'  )
									
						
					) m
								WHERE m.af_content_id !=''
			
						) n
				LATERAL VIEW explode(split(n.skus,',')) zqms as sku
				
				) x
				
			left  join 

		( select t.sku , t.skus,t.num,t.af_quantity from (
		
				SELECT 
					sku,
					n.skus as skus,
					num,
					n.af_quantity as af_quantity,
					event_name,
						event_value,
					event_time,
					platform,
					country_code
						FROM (SELECT
							m.event_name as event_name,
										m.event_value as event_value,
							m.event_time as event_time,
							m.af_content_id as skus,
							m.af_quantity as af_quantity,
							m.platform as platform,
										m.country_code as country_code
							FROM
							(
								SELECT
								event_name,
												event_value,
								event_time,
								case when substr(get_json_object(event_value, '$.af_content_id'),-1,1)=',' then
								substr(get_json_object(event_value, '$.af_content_id'),0,length(get_json_object(event_value, '$.af_content_id'))-1) 
								else get_json_object(event_value, '$.af_content_id') end as af_content_id,
								case when substr(get_json_object(event_value, '$.af_quantity'),-1,1)=',' then
								substr(get_json_object(event_value, '$.af_quantity'),0,length(get_json_object(event_value, '$.af_quantity'))-1) 
								else get_json_object(event_value, '$.af_quantity') end as af_quantity,
								platform,
												country_code
								FROM ods.ods_app_burial_log
								WHERE
								concat(year,month,day) = '${ADD_TIME}'
								and site='zaful'
								and get_json_object(event_value,'$.af_content_id') is not null
								and get_json_object(event_value,'$.af_content_id') != ''
								AND  get_json_object(event_value, '$.af_inner_mediasource') like 'category_%'
								--and get_json_object(event_value, '$.af_inner_mediasource') !='category_(null)'
								AND (event_name='af_add_to_wishlist' OR event_name='af_add_to_bag' OR event_name='af_view_product'  OR event_name='af_impression' OR event_name='af_create_order_success' OR event_name='af_purchase' )
					
							) m
								WHERE m.af_content_id !=''
			
						) n
				LATERAL VIEW explode(split(n.skus,',')) zqms as sku
				
				LATERAL VIEW explode(split(n.af_quantity,',')) zqms as num
				) t
				
				group by t.sku , t.skus,t.num,t.af_quantity
				) y
				on x.sku=y.sku and x.skus=y.skus and x.af_quantity=y.af_quantity

			) as a
		where a.sku !=''
		GROUP BY a.sku,a.platform,a.country_code

		) j
			
		left join (select goods_sn,cat_id from ods.ods_m_zaful_eload_goods where dt='${ADD_TIME}' group by goods_sn,cat_id) k on j.sku=k.goods_sn
		full join dw_proj.app_zaful_sku_cat_id_af_content_af_quantity_stg_create_wishlist_category p on j.sku=p.sku and j.platform=p.platform and j.country_code=p.country_code
		full join dw_proj.app_zaful_sku_cat_id_af_content_af_quantity_stg_purchase_wishlist_category q on j.sku=q.sku and j.platform=q.platform and j.country_code=q.country_code

) c
left join (select goods_sn,cat_id from ods.ods_m_zaful_eload_goods where dt='${ADD_TIME}' group by goods_sn,cat_id) k on c.sku=k.goods_sn
;  
  
 --输出结果表
  create table if not exists dw_proj.app_zaful_sku_cat_id_country_category_wuc_wishlist(
 sku                        STRING    comment '商品id',
 af_impression              INT       comment '曝光量',
 af_view_product            INT       comment '点击量',
 af_add_to_wishlist         	INT   COMMENT '收藏量',
 af_add_to_bag_user              INT       comment '加购量按事件',
 af_add_to_bag_num              INT       comment '加购量按数量',
 af_create_order_success_user   int        comment '创建订单按事件',
 af_create_order_success_num   int        comment '创建订单按数量',
 af_purchase_user   int        comment '支付订单按事件',
 af_purchase_num   int        comment '支付订单按数量',
 gmv_user   decimal(10,4)        comment '',
 gmv_num   decimal(10,4)        comment '',
 sale_value_user   decimal(10,4)        comment '',
 sale_value_num   decimal(10,4)        comment '',
 event_time                 INT       comment '时间戳',
 date                       int       comment '时间',       
 platform                   STRING    comment '平台',
 cat_id                     int       comment '分类id',
 country_code               STRING    comment '国家代码'
 )
 PARTITIONED BY (add_time STRING)
 ROW FORMAT DELIMITED FIELDS TERMINATED BY '\u0001' LINES TERMINATED BY '\n' STORED AS TEXTFILE
 ;

insert overwrite table dw_proj.app_zaful_sku_cat_id_country_category_wuc_wishlist PARTITION (add_time='${ADD_TIME}')
select 
sku,
sum(af_impression),
sum(af_view_product),
sum(af_add_to_wishlist),
sum(af_add_to_bag_user),
sum(af_add_to_bag_num),
sum(af_create_order_success_user),
sum(af_create_order_success_num),
sum(af_purchase_user),
sum(af_purchase_num),
sum(gmv_user),
sum(gmv_num),
sum(sale_value_user),
sum(sale_value_num),
event_time,date,platform,cat_id,country_code
from 
dw_proj.app_zaful_sku_cat_id_country_category_wuc_wishlist_temp
where add_time='${ADD_TIME}'
group by sku,event_time,date,platform,cat_id,country_code
;



























--总的
--输出结果表
  create table if not exists dw_proj.app_zaful_sku_cat_id_country_all_wuc_wishlist_temp(
 sku                        STRING    comment '商品id',
 af_impression              INT       comment '曝光量',
 af_view_product            INT       comment '点击量',
 af_add_to_wishlist         	INT   COMMENT '收藏量',
 af_add_to_bag_user              INT       comment '加购量按事件',
 af_add_to_bag_num              INT       comment '加购量按数量',
 af_create_order_success_user   int        comment '创建订单按事件',
 af_create_order_success_num   int        comment '创建订单按数量',
 af_purchase_user   int        comment '支付订单按事件',
 af_purchase_num   int        comment '支付订单按数量',
   gmv_user   decimal(10,4)        comment '',
 gmv_num   decimal(10,4)        comment '',
 sale_value_user   decimal(10,4)        comment '',
 sale_value_num   decimal(10,4)        comment '',
 event_time                 INT       comment '时间戳',
 date                       int       comment '时间',       
 platform                   STRING    comment '平台',
 cat_id                     int       comment '分类id',
 country_code               STRING    comment '国家代码'
 )
 PARTITIONED BY (add_time STRING)
 ROW FORMAT DELIMITED FIELDS TERMINATED BY '\u0001' LINES TERMINATED BY '\n' STORED AS TEXTFILE
 ;
 
 
 
 --执行查询，将结果写入hive表分区
insert overwrite table dw_proj.app_zaful_sku_cat_id_country_all_wuc_wishlist_temp PARTITION (add_time='${ADD_TIME}')
select
c.sku,
c.af_impression,
c.af_view_product,
c.af_add_to_wishlist,
c.af_add_to_bag,
c.af_add_to_bag_num,
c.af_create_order_success_user,
c.af_create_order_success_num,
c.af_purchase_user,
c.af_purchase_num,
c.gmv_user,
c.gmv_num,
c.sale_value_user,
c.sale_value_num,
c.add_times,
c.date,
c.platform,
k.cat_id,
c.country_code
from 
(
			SELECT
			coalesce(j.sku,p.sku,q.sku,'') as sku,
			j.af_impression as af_impression,
			j.af_view_product as af_view_product,
			j.af_add_to_wishlist as af_add_to_wishlist,
			j.af_add_to_bag_user as af_add_to_bag,
			j.af_add_to_bag_num as af_add_to_bag_num,
			p.create_order_count as af_create_order_success_user,
			p.create_order_sum as af_create_order_success_num,
			q.purchase_count as af_purchase_user,
			q.purchase_sum as af_purchase_num,
			p.gmv_user as gmv_user,
			p.gmv_num as gmv_num,
			q.sale_value_user as sale_value_user,
			q.sale_value_num as sale_value_num,
			UNIX_TIMESTAMP('${ADD_TIME}','yyyyMMdd') as add_times,
			from_unixtime(UNIX_TIMESTAMP('${ADD_TIME}','yyyyMMdd'),'yyyyMMdd') as date,
			coalesce(j.platform,p.platform,q.platform,'') as platform,
			k.cat_id as cat_id,
			coalesce(j.country_code,p.country_code,q.country_code,'') as country_code
			FROM 

			(
			SELECT 
			a.sku as sku,a.platform as platform,a.country_code as country_code,
			count(case when a.event_name='af_view_product' and get_json_object(event_value, '$.af_changed_size_or_color') = 0 then 1 else null end) as af_view_product,
			count(case when a.event_name='af_impression' then 1 else null end) as af_impression,
			count(case when a.event_name='af_add_to_wishlist' then 1 else null end) as af_add_to_wishlist,
			count(case when a.event_name='af_add_to_bag'  then 1 else null end) as af_add_to_bag_user,
			sum(case when a.event_name='af_add_to_bag'  then a.num  else 0 end) as af_add_to_bag_num

			FROM
				(
					select x.sku,y.num as num,x.event_name,x.event_value,x.event_time,x.platform,x.country_code from 

						(SELECT 
							sku,
							n.skus as skus,
							n.af_quantity as af_quantity,
							event_name,
								event_value,
							event_time,
							platform,
							country_code
								FROM (SELECT
									m.event_name as event_name,
												m.event_value as event_value,
									m.event_time as event_time,
									m.af_content_id as skus,
									m.af_quantity as af_quantity,
									m.platform as platform,
												m.country_code as country_code
									FROM
									(
										SELECT
										event_name,
														event_value,
										event_time,
										case when substr(get_json_object(event_value, '$.af_content_id'),-1,1)=',' then
										substr(get_json_object(event_value, '$.af_content_id'),0,length(get_json_object(event_value, '$.af_content_id'))-1) 
										else get_json_object(event_value, '$.af_content_id') end as af_content_id,
										case when substr(get_json_object(event_value, '$.af_quantity'),-1,1)=',' then
										substr(get_json_object(event_value, '$.af_quantity'),0,length(get_json_object(event_value, '$.af_quantity'))-1) 
										else get_json_object(event_value, '$.af_quantity') end as af_quantity,
										platform,
														country_code
										FROM ods.ods_app_burial_log
										WHERE
										concat(year,month,day) = '${ADD_TIME}'
										and site='zaful'
										and get_json_object(event_value,'$.af_content_id') is not null
										and get_json_object(event_value,'$.af_content_id') != ''
										AND (event_name='af_add_to_wishlist' OR event_name='af_add_to_bag' OR event_name='af_view_product'  OR event_name='af_impression'   )
										AND  get_json_object(event_value, '$.af_inner_mediasource') !='unknow mediasource'
										--OR event_name='af_create_order_success' OR event_name='af_purchase'
							
						) m
									WHERE m.af_content_id !=''
				
							) n
					LATERAL VIEW explode(split(n.skus,',')) zqms as sku
					
					) x
					
				left  join 

			( select t.sku , t.skus,t.num,t.af_quantity from (
			
					SELECT 
						sku,
						n.skus as skus,
						num,
						n.af_quantity as af_quantity,
						event_name,
							event_value,
						event_time,
						platform,
						country_code
							FROM (SELECT
								m.event_name as event_name,
											m.event_value as event_value,
								m.event_time as event_time,
								m.af_content_id as skus,
								m.af_quantity as af_quantity,
								m.platform as platform,
											m.country_code as country_code
								FROM
								(
									SELECT
									event_name,
													event_value,
									event_time,
									case when substr(get_json_object(event_value, '$.af_content_id'),-1,1)=',' then
									substr(get_json_object(event_value, '$.af_content_id'),0,length(get_json_object(event_value, '$.af_content_id'))-1) 
									else get_json_object(event_value, '$.af_content_id') end as af_content_id,
									case when substr(get_json_object(event_value, '$.af_quantity'),-1,1)=',' then
									substr(get_json_object(event_value, '$.af_quantity'),0,length(get_json_object(event_value, '$.af_quantity'))-1) 
									else get_json_object(event_value, '$.af_quantity') end as af_quantity,
									platform,
									country_code
									FROM ods.ods_app_burial_log
									WHERE
									concat(year,month,day) = '${ADD_TIME}'
									and site='zaful'
									and get_json_object(event_value,'$.af_content_id') is not null
									and get_json_object(event_value,'$.af_content_id') != ''
									AND (event_name='af_add_to_wishlist' OR event_name='af_add_to_bag' OR event_name='af_view_product'  OR event_name='af_impression' )
									AND  get_json_object(event_value, '$.af_inner_mediasource') !='unknow mediasource'
											--OR event_name='af_create_order_success' OR event_name='af_purchase' 
									
								) m
									WHERE m.af_content_id !=''
				
							) n
					LATERAL VIEW explode(split(n.skus,',')) zqms as sku
					
					LATERAL VIEW explode(split(n.af_quantity,',')) zqms as num
					) t
					
					group by t.sku , t.skus,t.num,t.af_quantity
					) y
					on x.sku=y.sku and x.skus=y.skus and x.af_quantity=y.af_quantity

				) as a
			where a.sku !=''
			GROUP BY a.sku,a.platform,a.country_code

			) j
				
			left join (select goods_sn,cat_id from ods.ods_m_zaful_eload_goods where dt='${ADD_TIME}' group by goods_sn,cat_id) k on j.sku=k.goods_sn

			full join dw_proj.app_zaful_sku_cat_id_af_content_af_quantity_stg_create_wishlist p on j.sku=p.sku and j.platform=p.platform and j.country_code=p.country_code

			full join dw_proj.app_zaful_sku_cat_id_af_content_af_quantity_stg_purchase_wishlist q on j.sku=q.sku and j.platform=q.platform and j.country_code=q.country_code
) c
left join (select goods_sn,cat_id from ods.ods_m_zaful_eload_goods where dt='${ADD_TIME}' group by goods_sn,cat_id) k on c.sku=k.goods_sn
;
  
--总的
--输出结果表
  create table if not exists dw_proj.app_zaful_sku_cat_id_country_all_wuc_wishlist(
 sku                        STRING    comment '商品id',
 af_impression              INT       comment '曝光量',
 af_view_product            INT       comment '点击量',
 af_add_to_wishlist         	INT   COMMENT '收藏量',
 af_add_to_bag_user              INT       comment '加购量按事件',
 af_add_to_bag_num              INT       comment '加购量按数量',
 af_create_order_success_user   int        comment '创建订单按事件',
 af_create_order_success_num   int        comment '创建订单按数量',
 af_purchase_user   int        comment '支付订单按事件',
 af_purchase_num   int        comment '支付订单按数量',
   gmv_user   decimal(10,4)        comment '',
 gmv_num   decimal(10,4)        comment '',
 sale_value_user   decimal(10,4)        comment '',
 sale_value_num   decimal(10,4)        comment '',
 event_time                 INT       comment '时间戳',
 date                       int       comment '时间',       
 platform                   STRING    comment '平台',
 cat_id                     int       comment '分类id',
 country_code               STRING    comment '国家代码'
 )
 PARTITIONED BY (add_time STRING)
 ROW FORMAT DELIMITED FIELDS TERMINATED BY '\u0001' LINES TERMINATED BY '\n' STORED AS TEXTFILE
 ;


insert overwrite table dw_proj.app_zaful_sku_cat_id_country_all_wuc_wishlist PARTITION (add_time='${ADD_TIME}')
select 
sku,
sum(af_impression),
sum(af_view_product),
sum(af_add_to_wishlist),
sum(af_add_to_bag_user),
sum(af_add_to_bag_num),
sum(af_create_order_success_user),
sum(af_create_order_success_num),
sum(af_purchase_user),
sum(af_purchase_num),
sum(gmv_user),
sum(gmv_num),
sum(sale_value_user),
sum(sale_value_num),
event_time,date,platform,cat_id,country_code
from 
dw_proj.app_zaful_sku_cat_id_country_all_wuc_wishlist_temp
where add_time='${ADD_TIME}'
group by sku,event_time,date,platform,cat_id,country_code
;
  
  
  