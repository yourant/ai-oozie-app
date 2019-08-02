
--@author wuchao
--@date 2018年12月20日 
--@desc  Zaful App新需求以sku,cat_id导入mongodb


SET mapred.job.name=zaful_recommend_cold_start_fb;
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
set hive.support.concurrency=false;                
                       
 --输出结果表
  create table if not exists dw_proj.zaful_recommend_cold_start_order_count(
 sku                  STRING       comment '',
 user_id               STRING       comment '',
 platform                  STRING       comment '',
 country_code               STRING       comment '',
 order_count                 STRING       comment ''
 )
 PARTITIONED BY (add_time STRING)
 ROW FORMAT DELIMITED FIELDS TERMINATED BY '\u0001' LINES TERMINATED BY '\n' STORED AS TEXTFILE
 ;                  

insert overwrite table dw_proj.zaful_recommend_cold_start_order_count PARTITION (add_time='${ADD_TIME}')
select
sku,user_id,platform,region_code,count(distinct order_sn) as order_count
from (
SELECT
  p.goods_sn as sku,
  x.user_id,
  x.order_sn as order_sn,
  case when x.order_sn LIKE 'UA%'  OR   x.order_sn   LIKE 'UUA%' THEN 'ios'  
  WHEN x.order_sn LIKE 'UB%'  OR   x.order_sn   LIKE 'UUB%' THEN 'android'  end
  as platform,
  x.region_code as region_code
FROM
  (   
    select a.order_id,a.user_id,a.order_sn,a.region_code from 
    (
        select 
        f.order_id as order_id,
        f.user_id as user_id,
        f.order_sn as order_sn,
        coalesce(g.region_code,'') as region_code  
        from 
        (
        SELECT
          order_id,order_sn,country,user_id
        FROM
          ods.ods_m_zaful_eload_order_info
        WHERE
          from_unixtime(add_time+8*3600, 'yyyy-MM-dd') = '${ADD_TIME}'
          AND (order_sn LIKE 'UA%'  OR   order_sn   LIKE 'UUA%' 
          OR	order_sn LIKE 'UB%'  OR   order_sn   LIKE 'UUB%' )
          and dt=concat('${YEAR}','${MONTH}','${DAY}')
          --and dt='20190317'
        ) f
        left join dw_zaful_recommend.ods_zaful_eload_region g on f.country=g.region_id
    ) a 
    group by a.order_id,a.user_id,a.order_sn,a.region_code
  ) x
  JOIN
  (
  SELECT goods_sn,order_id from ods.ods_m_zaful_eload_order_goods
  where 
  dt=concat('${YEAR}','${MONTH}','${DAY}')
    --dt='20190317'
  ) p
   ON x.order_id = p.order_id
) as m  
GROUP BY m.sku,m.platform,m.region_code,m.user_id
;

create table if not exists dw_proj.zaful_recommend_cold_start_user_id_ods(
  fb_adset_id               STRING       comment '',
  user_id               STRING       comment '',
 platform                  STRING       comment '',
 country_code                  STRING       comment ''
 )
 PARTITIONED BY (add_time STRING)
 ROW FORMAT DELIMITED FIELDS TERMINATED BY '\u0001' LINES TERMINATED BY '\n' STORED AS TEXTFILE
 ;     

 insert overwrite table dw_proj.zaful_recommend_cold_start_user_id_ods PARTITION (add_time='${ADD_TIME}')
  select 
    fb_adset_id,
    customer_user_id as user_id,
    platform,
    country_code
  from
    ods.ods_app_burial_log
  where
    concat(year,'-',month,'-',day) = '${ADD_TIME}'
    and site='zaful'
    and fb_adset_id is not null and fb_adset_id !=''
  group by 
    fb_adset_id,
    customer_user_id,
    platform,
    country_code
  ;

create table if not exists dw_proj.zaful_recommend_cold_start_user_id_order(
 sku               STRING       comment '',
 fb_adset_id               STRING       comment '',
 customer_user_id           STRING       comment '',
 platform                   STRING       comment '',
 country_code               STRING       comment '',
 order_count               STRING       comment ''
 )
 PARTITIONED BY (add_time STRING)
 ROW FORMAT DELIMITED FIELDS TERMINATED BY '\u0001' LINES TERMINATED BY '\n' STORED AS TEXTFILE
 ;     

insert overwrite table dw_proj.zaful_recommend_cold_start_user_id_order PARTITION (add_time='${ADD_TIME}')
select a.sku,b.fb_adset_id,a.user_id,a.platform,a.country_code,a.order_count 
from
  (select sku,user_id,platform,country_code,order_count from dw_proj.zaful_recommend_cold_start_order_count where add_time = '${ADD_TIME}') a
  left join 
  (select fb_adset_id,user_id,platform,country_code from dw_proj.zaful_recommend_cold_start_user_id_ods where add_time = '${ADD_TIME}') b
  on   a.user_id=b.user_id and a.country_code=b.country_code and a.platform=b.platform
;