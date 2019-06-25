
--@author wuchao
--@date 2018年12月20日 
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
set hive.support.concurrency=false;                
                       
 --输出结果表
  create table if not exists dw_proj.app_zaful_sku_cat_id_af_content_af_quantity_stg_create_wishlist(
 sku                  STRING       comment '',
 create_order_count               STRING       comment '',
 create_order_sum                STRING       comment '',
 gmv_user             STRING       comment '',
 gmv_num               STRING       comment '',
 platform                  STRING       comment '',
 country_code               STRING       comment ''
 )
 ROW FORMAT DELIMITED FIELDS TERMINATED BY '\u0001' LINES TERMINATED BY '\n' STORED AS TEXTFILE
 ;                  

  --输出结果表
  create table if not exists dw_proj.app_zaful_sku_cat_id_af_content_af_quantity_stg_purchase_wishlist(
 sku                  STRING       comment '',
 purchase_count             STRING       comment '',
 purchase_sum               STRING       comment '',
 sale_value_user              STRING       comment '',
 sale_value_num                STRING       comment '',
 platform                  STRING       comment '',
  country_code               STRING       comment ''
 )
 ROW FORMAT DELIMITED FIELDS TERMINATED BY '\u0001' LINES TERMINATED BY '\n' STORED AS TEXTFILE
 ;               
                       
insert overwrite table dw_proj.app_zaful_sku_cat_id_af_content_af_quantity_stg_create_wishlist    
select
sku,count(order_sn),sum(goods_number),sum(m.pay_amount)/sum(goods_number) * count(order_sn),sum(m.pay_amount),platform,country_code
from (
SELECT
  p.goods_sn as sku,
  x.order_sn as order_sn,
  p.goods_number as goods_number,
  p.pay_amount as pay_amount,
  case when x.order_sn LIKE 'UA%'  OR   x.order_sn   LIKE 'UUA%' THEN 'ios'  
  WHEN x.order_sn LIKE 'UB%'  OR   x.order_sn   LIKE 'UUB%' THEN 'android'  end
  as platform,
  x.region_code as country_code
FROM
  (
    select 
    f.order_id as order_id,
    f.add_time as add_time,
    f.order_sn as order_sn,
     g.region_code as region_code
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
      from_unixtime(add_time+8*3600, 'yyyyMMdd') = '${ADD_TIME}'
      AND (order_sn LIKE 'UA%'  OR   order_sn   LIKE 'UUA%' 
		   OR	order_sn LIKE 'UB%'  OR   order_sn   LIKE 'UUB%' )
       and 
       dt='${ADD_TIME}'
       -- dt='20190313'
    ) f
   left join stg.zaful_eload_region g on f.country=g.region_id

  ) x
  JOIN
  (
  SELECT goods_sn,order_id,goods_number,
    case when goods_pay_amount <> '0' and  goods_pay_amount is not null then goods_pay_amount 
  else goods_price*goods_number end as pay_amount
  from
   ods.ods_m_zaful_eload_order_goods 
   where 
   dt= '${ADD_TIME}'
   --dt='20190313'
   ) p
   ON x.order_id = p.order_id
) as m
GROUP BY m.sku,m.platform,m.country_code    
;         


-- and order_status not in ('0','11','13')
insert overwrite table dw_proj.app_zaful_sku_cat_id_af_content_af_quantity_stg_purchase_wishlist    
select
sku,count(order_sn),sum(goods_number),sum(m.pay_amount)/sum(goods_number) * count(order_sn),sum(m.pay_amount),platform,country_code
from (
SELECT
  p.goods_sn as sku,
  x.order_sn as order_sn,
  p.goods_number as goods_number,
  p.pay_amount as pay_amount,
  case when x.order_sn LIKE 'UA%'  OR   x.order_sn   LIKE 'UUA%' THEN 'ios'  
  WHEN x.order_sn LIKE 'UB%'  OR   x.order_sn   LIKE 'UUB%' THEN 'android'  end
  as platform,
  x.region_code as country_code
FROM
  (
    select 
    f.order_id as order_id,
    f.add_time as add_time,
    f.order_sn as order_sn,
     g.region_code as region_code
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
      from_unixtime(add_time+8*3600, 'yyyyMMdd') = '${ADD_TIME}'
      AND (order_sn LIKE 'UA%'  OR   order_sn   LIKE 'UUA%' 
		   OR	order_sn LIKE 'UB%'  OR   order_sn   LIKE 'UUB%' )
       and order_status not in ('0','11','13')
       and 
       dt='${ADD_TIME}'
       --dt='20190313'
    ) f
   left join stg.zaful_eload_region g on f.country=g.region_id

  ) x
  JOIN
  (
  SELECT goods_sn,order_id,goods_number,
      case when goods_pay_amount <> '0' and  goods_pay_amount is not null then goods_pay_amount 
  else goods_price*goods_number end as pay_amount
  from
   ods.ods_m_zaful_eload_order_goods 
   where 
   dt= '${ADD_TIME}'
   --dt='20190313'
   ) p
   ON x.order_id = p.order_id
) as m
   
GROUP BY m.sku,m.platform,m.country_code    
; 





CREATE TABLE IF NOT EXISTS dw_zaful_recommend.zaful_app_category_xxx_all_sku_user_country_tmp_second(
goods_sn                  STRING         COMMENT "sku",
user_id                   STRING         COMMENT "用户ID",
platform                  STRING         COMMENT "平台",
country_code              STRING         COMMENT ""
)
COMMENT "列表页报表user-sku-county中间表"
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\u0001' LINES TERMINATED BY '\n' STORED AS TEXTFILE;


--加购sku，user_id
INSERT OVERWRITE TABLE dw_zaful_recommend.zaful_app_category_xxx_all_sku_user_country_tmp_second
SELECT
  m.goods_sn,
  n.customer_user_id,
  m.platform,
  m.country_code
FROM
  (
    SELECT
      appsflyer_device_id,
      --regexp_extract(event_value, '(.*?sku":")([0-9a-zA-Z]*)(".*?)', 2) AS sku,
      get_json_object(event_value, '$.af_content_id') AS goods_sn,
      platform,
      get_json_object(event_value, '$.af_inner_mediasource') AS af_inner_mediasource,
      language,
      country_code
    FROM
      ods.ods_app_burial_log 
    WHERE
      concat(year, month, day) BETWEEN '${ADD_TIME_W}'
      AND '${ADD_TIME}'
      AND event_name='af_add_to_bag'
      and site='zaful'
      AND get_json_object(event_value, '$.af_content_id') is not null 
      and get_json_object(event_value, '$.af_inner_mediasource')  like 'category_%'
  ) m
  INNER JOIN dw_zaful_recommend.zaful_app_u_map n ON m.appsflyer_device_id = n.appsflyer_device_id
GROUP BY
  m.goods_sn,
  n.customer_user_id,
  m.platform,
  m.country_code
;

CREATE TABLE IF NOT EXISTS dw_zaful_recommend.zaful_app_category_xxx_all_order_info_good_tmp_order_sn_second(
goods_sn                  STRING         COMMENT "sku",
user_id                   STRING         COMMENT "用户ID",
order_status              STRING         COMMENT "订单状态",
pay_amount                decimal(10,2)  COMMENT "购买金额",
goods_number              STRING         COMMENT "商品数量",
order_sn                  STRING         COMMENT "订单编号",
country                   STRING         COMMENT "国家"
)
COMMENT "列表页报表user-sku中间表"
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\u0001' LINES TERMINATED BY '\n' STORED AS TEXTFILE;

--订单表
INSERT OVERWRITE TABLE dw_zaful_recommend.zaful_app_category_xxx_all_order_info_good_tmp_order_sn_second
SELECT
  p.goods_sn,
  x.user_id,
  x.order_status,
  p.pay_amount,
  p.goods_number,
  x.order_sn,
  x.country
FROM
  (
    SELECT
      order_id,
      user_id,
      add_time,
      order_status,
      order_sn,
      country
    FROM
      ods.ods_m_zaful_eload_order_info
    WHERE
      from_unixtime(add_time+8*3600, 'yyyyMMdd') = '${ADD_TIME}'
      AND (order_sn LIKE 'UA%'  OR   order_sn   LIKE 'UUA%' 
		   OR	order_sn LIKE 'UB%'  OR   order_sn   LIKE 'UUB%' )
      and 
      dt='${ADD_TIME}'
      --dt='20190313'
  ) x
  JOIN
  (
  SELECT goods_sn,order_id,
  goods_number,
  case when goods_pay_amount <> '0' and  goods_pay_amount is not null then goods_pay_amount
  else goods_price*goods_number end as pay_amount
  from
   ods.ods_m_zaful_eload_order_goods where 
   dt='${ADD_TIME}'
   --dt='20190313'
   ) p
   ON x.order_id = p.order_id
;

--列表页订单表
create table if not exists dw_proj.zaful_app_fourteen_days_category_order_info_sku_second(
 sku                  STRING       comment '',
 order_sn             STRING       comment '',
 goods_number               INT       comment '',
 pay_amount              decimal(10,4)       comment '',
 order_status          STRING       COMMENT '',
 country              STRING       COMMENT ''
 )
 ROW FORMAT DELIMITED FIELDS TERMINATED BY '\u0001' LINES TERMINATED BY '\n' STORED AS TEXTFILE
 ;     

insert overwrite table dw_proj.zaful_app_fourteen_days_category_order_info_sku_second 
SELECT
  x2.goods_sn as sku,
  x2.order_sn,
  x2.goods_number,
  x2.pay_amount,
  x2.order_status,
  x1.country_code
from
  dw_zaful_recommend.zaful_app_category_xxx_all_sku_user_country_tmp_second x1
  INNER JOIN dw_zaful_recommend.zaful_app_category_xxx_all_order_info_good_tmp_order_sn_second x2 ON x1.user_id = x2.user_id
  AND x1.goods_sn = x2.goods_sn
;



 --列表页输出结果表
create table if not exists dw_proj.app_zaful_sku_cat_id_af_content_af_quantity_stg_create_wishlist_category(
 sku                  STRING       comment '',
 create_order_count               STRING       comment '',
 create_order_sum                STRING       comment '',
 gmv_user             STRING       comment '',
 gmv_num               STRING       comment '',
 platform                  STRING       comment '',
 country_code               STRING       comment ''
 )
 ROW FORMAT DELIMITED FIELDS TERMINATED BY '\u0001' LINES TERMINATED BY '\n' STORED AS TEXTFILE
 ;                  

  --列表页输出结果表
create table if not exists dw_proj.app_zaful_sku_cat_id_af_content_af_quantity_stg_purchase_wishlist_category(
 sku                  STRING       comment '',
 purchase_count             STRING       comment '',
 purchase_sum               STRING       comment '',
 sale_value_user              STRING       comment '',
 sale_value_num                STRING       comment '',
 platform                  STRING       comment '',
  country_code               STRING       comment ''
 )
 ROW FORMAT DELIMITED FIELDS TERMINATED BY '\u0001' LINES TERMINATED BY '\n' STORED AS TEXTFILE
 ;    

                        
insert overwrite table dw_proj.app_zaful_sku_cat_id_af_content_af_quantity_stg_create_wishlist_category    
select
sku,count(order_sn),sum(goods_number),sum(m.pay_amount)/sum(goods_number) * count(order_sn),sum(m.pay_amount),platform,country_code
from (
SELECT
  x.sku as sku,
  x.order_sn as order_sn,
  x.goods_number as goods_number,
  x.pay_amount as pay_amount,
  case when x.order_sn LIKE 'UA%'  OR   x.order_sn   LIKE 'UUA%' THEN 'ios'  
  WHEN x.order_sn LIKE 'UB%'  OR   x.order_sn   LIKE 'UUB%' THEN 'android'  end
  as platform,
  x.region_code as country_code
FROM
  (
    select 
    f.sku as sku,
    f.order_sn as order_sn,
    f.goods_number as goods_number,
    f.pay_amount as pay_amount,
    f.order_status as order_status,
    f.country_code as region_code
    from 
    (
      SELECT
      sku,
      order_sn,
      goods_number,
      pay_amount,
      order_status,
      country as country_code
    FROM
      dw_proj.zaful_app_fourteen_days_category_order_info_sku_second
    ) f
   --left join stg.zaful_eload_region g on f.country=g.region_id

  ) x
) as m
   
GROUP BY m.sku,m.platform,m.country_code    
;         


-- and order_status not in ('0','11','13')
insert overwrite table dw_proj.app_zaful_sku_cat_id_af_content_af_quantity_stg_purchase_wishlist_category    
select
sku,count(order_sn),sum(goods_number),sum(m.pay_amount)/sum(goods_number) * count(order_sn),sum(m.pay_amount),platform,country_code
from (
SELECT
  x.sku as sku,
  x.order_sn as order_sn,
  x.goods_number as goods_number,
  x.pay_amount as pay_amount,
  case when x.order_sn LIKE 'UA%'  OR   x.order_sn   LIKE 'UUA%' THEN 'ios'  
  WHEN x.order_sn LIKE 'UB%'  OR   x.order_sn   LIKE 'UUB%' THEN 'android'  end
  as platform,
  x.region_code as country_code
FROM
  (
    select 
    f.sku as sku,
    f.order_sn as order_sn,
    f.goods_number as goods_number,
    f.pay_amount as pay_amount,
    f.order_status as order_status,
    f.country_code as region_code
    from 
    (
      SELECT
      sku,
      order_sn,
      goods_number,
      pay_amount,
      order_status,
      country as country_code
    FROM
      dw_proj.zaful_app_fourteen_days_category_order_info_sku_second
    where 
      order_status not in ('0','11','13')
    ) f
   --left join stg.zaful_eload_region g on f.country=g.region_id

  ) x
) as m
GROUP BY m.sku,m.platform,m.country_code    
;  