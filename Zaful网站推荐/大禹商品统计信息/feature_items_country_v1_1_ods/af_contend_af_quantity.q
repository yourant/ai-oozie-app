
--@author wuchao
--@date 2018年12月20日 
--@desc  Zaful App新需求以sku,cat_id导入mongodb


SET mapred.job.name=feature_items_country_v2_2_ods;
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
  create table if not exists dw_proj.feature_items_country_v2_2_ods_order(
 sku                  STRING       comment '',
 create_order_sum                STRING       comment '',
 gmv_num               STRING       comment '',
 platform                  STRING       comment '',
 country_code            STRING       comment ''
 )
 ROW FORMAT DELIMITED FIELDS TERMINATED BY '\u0001' LINES TERMINATED BY '\n' STORED AS TEXTFILE
 ;                  

    




insert overwrite table dw_proj.feature_items_country_v2_2_ods_order    
select
sku,sum(goods_number),sum(m.pay_amount),platform,country_code
from (
SELECT
  p.goods_sn as sku,
  x.order_sn as order_sn,
  p.goods_number as goods_number,
  p.pay_amount as pay_amount,
  case when x.order_sn LIKE 'UU1%' OR   x.order_sn   LIKE 'U1%'  THEN 'pc'
       WHEN x.order_sn LIKE 'UL%'  OR   x.order_sn   LIKE 'UM%'  THEN 'm'
       WHEN x.order_sn LIKE 'UA%'  OR   x.order_sn   LIKE 'UUA%' THEN 'ios'
       WHEN x.order_sn LIKE 'UB%'  OR   x.order_sn   LIKE 'UUB%' THEN 'android'
  else 'null' end as platform,
  x.country_code
FROM
  (
    select 
    f.order_id as order_id,
    f.add_time as add_time,
    f.order_sn as order_sn,
    g.region_code as country_code
    from 
    (
    SELECT
      order_id,
      add_time,
      order_sn,
      country
    FROM
      ods.ods_m_zaful_eload_order_info
    WHERE
      from_unixtime(add_time+8*3600, 'yyyyMMdd') = '${DATE}'
      and (order_sn LIKE 'UU1%' OR   order_sn   LIKE 'U1%'  OR 
      order_sn LIKE 'UL%'  OR   order_sn   LIKE 'UM%'  OR 
      order_sn LIKE 'UA%'  OR   order_sn   LIKE 'UUA%' OR
      order_sn LIKE 'UB%'  OR   order_sn   LIKE 'UUB%' )
       and dt='${DATE}'
    ) f
    left join dw_zaful_recommend.ods_zaful_eload_region g on f.country=g.region_id
  ) x
  inner JOIN
  (
  SELECT goods_sn,order_id,1 as goods_number,
  case when goods_pay_amount <> '0' and  goods_pay_amount is not null then goods_pay_amount 
  else goods_price*goods_number end as pay_amount
  from
   ods.ods_m_zaful_eload_order_goods where dt= '${DATE}') p
   ON x.order_id = p.order_id
) as m  
GROUP BY m.sku,m.platform,country_code    
;         


