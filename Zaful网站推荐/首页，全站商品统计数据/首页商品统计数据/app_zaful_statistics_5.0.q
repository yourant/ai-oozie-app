--@author wuchao
--@date 2018年09月30日 
--@desc  zaful App端推荐位报表按sku统计曝光，点击，加购，加收藏，订单等

SET mapred.job.name=gb_recommend_position_report;
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


 

 
 
 --创建最终结果表
create table if not exists dw_proj.app_zaful_sku_statistics_country_recommend_homepage(
 sku                        STRING    comment '商品id',
 platform                   STRING    comment '来源区分,com.zaful表示来自安卓',
 country_code               STRING    comment '国家代码',
 af_inner_mediasource       STRING    comment '推荐位',
 af_add_to_bag              INT       comment '加入购物车事件',
 af_view_product            INT       comment '进入产品详情页查看指定sku商品',
 af_add_to_wishlist         INT       comment '添加商品至收藏夹',
 af_impression              INT       comment '列表商品展示，来源会有多处，如首页推荐位，商品推荐位等',
 add_times                   INT       comment '时间分区',
 sites                       STRING    comment 'site分区'
 )
 PARTITIONED BY (add_time STRING,site STRING)
 ROW FORMAT DELIMITED FIELDS TERMINATED BY '\u0001' LINES TERMINATED BY '\n' STORED AS TEXTFILE
 ;
 
 
 
 --执行查询，将结果写入hive表分区
insert into table dw_proj.app_zaful_sku_statistics_country_recommend_homepage PARTITION (add_time='${ADD_TIME}',site='zaful')
SELECT
j.sku as sku,
j.platform as platform,
j.country_code as country_code,
'recommend_homepage' as af_inner_mediasource,
j.af_add_to_bag as af_add_to_bag,
j.af_view_product as af_view_product,
j.af_add_to_wishlist as af_add_to_wishlist,
j.af_impression as af_impression,
UNIX_TIMESTAMP('${ADD_TIME}','yyyy-MM-dd') as add_times,
'zaful' as site
FROM 

(
SELECT 
a.sku as sku,a.platform as platform,a.country_code as country_code,
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
  country_code
    FROM (SELECT
			m.event_name as event_name,
      m.event_value as event_value,
			m.af_content_id as skus,
			m.platform as platform,
      m.country_code as country_code
			FROM
			(
				SELECT
				event_name,
        event_value,
				get_json_object(event_value,'$.af_content_id') as af_content_id,
				platform,
        country_code
				FROM ods.ods_app_burial_log
				WHERE
				concat_ws('-',year,month,day) = '${ADD_TIME}'
				and site='zaful'
        AND  get_json_object(event_value, '$.af_inner_mediasource') ='recommend_homepage'
				AND (event_name='af_add_to_bag' OR event_name='af_view_product' OR event_name='af_add_to_wishlist' OR event_name='af_impression' )
				
			) m
            WHERE m.af_content_id !=''
	
	      ) n
    LATERAL VIEW explode(split(n.skus,',')) zqms as sku
  ) as a
GROUP BY a.sku,a.platform,a.country_code

) j

;


 
 
 
 
 
 
 
 
 
 
 
 
 
 
 