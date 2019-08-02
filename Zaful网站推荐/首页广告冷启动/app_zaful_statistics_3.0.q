--@author wuchao
--@date 2018年09月30日 
--@desc  zaful App端推荐位报表按sku统计曝光，点击，加购，加收藏，订单等

SET mapred.job.name=zaful_recommend_cold_start_fb;
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


 

 
 

  create table if not exists dw_proj.app_zaful_sku_statistics_country_fb(
 sku                        STRING    comment '商品id',
 platform                   STRING    comment '来源区分,com.zaful表示来自安卓',
 country_code               STRING    comment '国家代码',
 fb_adset_id                STRING    comment 'fb广告id',
 af_add_to_bag              INT       comment '加入购物车事件',
 af_view_product            INT       comment '进入产品详情页查看指定sku商品',
 af_add_to_wishlist         INT       comment '添加商品至收藏夹',
 af_impression              INT       comment '列表商品展示，来源会有多处，如首页推荐位，商品推荐位等',
 order_count                INT       comment '订单数',
 add_times                  INT       comment '时间',
 sites                      STRING    comment 'site'
 )
 PARTITIONED BY (add_time STRING,site STRING)
 ROW FORMAT DELIMITED FIELDS TERMINATED BY '\u0001' LINES TERMINATED BY '\n' STORED AS TEXTFILE
 ;
 
 
 
 --执行查询，将结果写入hive表分区
insert overwrite table dw_proj.app_zaful_sku_statistics_country_fb PARTITION (add_time='${ADD_TIME}',site='zaful')
SELECT
coalesce(j.sku,k.sku,'') as sku,
coalesce(j.platform,k.platform,'') as platform,
coalesce(j.country_code,k.country_code,'') as country_code,
coalesce(j.fb_adset_id,k.fb_adset_id,'') as fb_adset_id,
j.af_add_to_bag as af_add_to_bag,
j.af_view_product as af_view_product,
j.af_add_to_wishlist as af_add_to_wishlist,
j.af_impression as af_impression,
coalesce(k.order_count,0) as order_count,
UNIX_TIMESTAMP('${ADD_TIME}','yyyy-MM-dd') as add_times,
'zaful' as site
FROM 

(
SELECT 
a.sku as sku,a.platform as platform,a.country_code as country_code,a.fb_adset_id,
count(case when a.event_name='af_add_to_bag'  then 1 else null end) as af_add_to_bag,
count(case when a.event_name='af_view_product' and get_json_object(event_value, '$.af_changed_size_or_color') = 0 then 1 else null end) as af_view_product,
count(case when a.event_name='af_add_to_wishlist' then 1 else null end) as af_add_to_wishlist,
count(case when a.event_name='af_impression' then 1 else null end) as af_impression
FROM
  ( SELECT 
	sku,
	event_name,
  event_value,
	platform,
  country_code,
  fb_adset_id
    FROM (SELECT
			m.event_name as event_name,
      m.event_value as event_value,
			m.af_content_id as skus,
			m.platform as platform,
      m.country_code as country_code,
      m.fb_adset_id
			FROM
			(
				SELECT
				event_name,
        event_value,
				get_json_object(event_value,'$.af_content_id') as af_content_id,
				platform,
        country_code,
        fb_adset_id
				FROM ods.ods_app_burial_log
				WHERE
				concat_ws('-',year,month,day) = '${ADD_TIME}'
				and site='zaful'
        AND  get_json_object(event_value, '$.af_inner_mediasource') !='unknow mediasource'
				AND (event_name='af_add_to_bag' OR event_name='af_view_product' OR event_name='af_add_to_wishlist' OR event_name='af_impression' )
				and fb_adset_id !='' and fb_adset_id is not null
			) m
            WHERE m.af_content_id !=''
	
	      ) n
    LATERAL VIEW explode(split(n.skus,',')) zqms as sku
  ) as a
GROUP BY a.sku,a.platform,a.country_code,a.fb_adset_id

) j
full join (select * from dw_proj.zaful_recommend_cold_start_user_id_order where add_time = '${ADD_TIME}' and fb_adset_id is not null) k
on j.sku=k.sku and j.platform=k.platform and j.country_code=k.country_code and j.fb_adset_id=k.fb_adset_id
;



  create table if not exists dw_proj.app_zaful_sku_statistics_country_fb_genders(
 sku                        STRING    comment '商品id',
 platform                   STRING    comment '来源区分,com.zaful表示来自安卓',
 country_code               STRING    comment '国家代码',
 genders                    STRING    comment '性别 0是不限，1是男性，2是女性',
 number_of_gender           INT       comment '性别数量',
 af_add_to_bag              INT       comment '加入购物车事件',
 af_view_product            INT       comment '进入产品详情页查看指定sku商品',
 af_add_to_wishlist         INT       comment '添加商品至收藏夹',
 af_impression              INT       comment '列表商品展示，来源会有多处，如首页推荐位，商品推荐位等',
 order_count                INT       comment '订单数',
 add_times                  INT       comment '时间',
 sites                      STRING    comment 'site'
 )
 PARTITIONED BY (add_time STRING,site STRING)
 ROW FORMAT DELIMITED FIELDS TERMINATED BY '\u0001' LINES TERMINATED BY '\n' STORED AS TEXTFILE
 ;


insert overwrite table dw_proj.app_zaful_sku_statistics_country_fb_genders PARTITION (add_time='${ADD_TIME}',site='zaful')
select 
c.sku,c.platform,c.country_code,c.genders,
count(genders),
sum(c.af_add_to_bag),
sum(c.af_view_product),
sum(c.af_add_to_wishlist),
sum(c.af_impression),
sum(c.order_count),
c.add_times,c.sites
from
(
    select a.sku,a.platform,a.country_code,coalesce(b.fieldvalue,0) as genders,a.af_add_to_bag,a.af_view_product,a.af_add_to_wishlist,a.af_impression,a.order_count,a.add_times,a.sites
    from 
    (select * from dw_proj.app_zaful_sku_statistics_country_fb where add_time='${ADD_TIME}' and fb_adset_id !='') a 
    left join 
    (
        select 
        adset_id,
        fieldvalue
        from
        ods.ods_s_ams_ams_test_tb_adset_details
        where
        fieldname in ('genders')
        group by adset_id,fieldvalue
    ) b
    on a.fb_adset_id=b.adset_id
) c
 group by c.sku,c.platform,c.country_code,c.genders,c.add_times,c.sites
;