--@author wuchao
--@date 2018年10月22日 
--@desc  zaful App端推荐位报表按sku统计曝光，点击，加购，加收藏，订单,订单金额等

SET mapred.job.name=zaful_app_recommend_position_report;
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


CREATE TABLE IF NOT EXISTS dw_zaful_recommend.rewrite_zaful_app_abtest_report(
exp_num                   INT            COMMENT "商品曝光数",
click_num                 INT            COMMENT "商品点击数",
cart_num                  INT            COMMENT "商品加购数",
collect_num               INT            COMMENT "商品收藏数",
order_num                 INT            COMMENT "订单数",
order_amount              INT            COMMENT "订单金额",
order_gmv                 INT            COMMENT "GMV",
platform                  STRING         COMMENT "android或者ios",
af_inner_mediasource      STRING         COMMENT "推荐位",
planid                    STRING         COMMENT "",
versionid                 STRING         comment ""
)
COMMENT 'ZAFUL APP AB测试数据报表'
PARTITIONED BY (add_time STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\u0001' LINES TERMINATED BY '\n' STORED AS TEXTFILE
;



--商品曝光数	
CREATE TABLE IF NOT EXISTS dw_zaful_recommend.rewrite_zaful_app_abtest_exp_num_tmp(
exp_num                INT            COMMENT "商品曝光数",
platform                  STRING         COMMENT "android或者ios",
af_inner_mediasource      STRING         COMMENT "推荐位",
planid                    STRING         COMMENT "",
versionid                 STRING         COMMENT "",
add_time                  STRING         COMMENT "时间"
)
COMMENT "商品曝光数"
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\u0001' LINES TERMINATED BY '\n' STORED AS TEXTFILE;



--商品点击数	
CREATE TABLE IF NOT EXISTS dw_zaful_recommend.rewrite_zaful_app_abtest_click_num_tmp(
click_num                 INT            COMMENT "商品点击数",
platform                  STRING         COMMENT "android或者ios",
af_inner_mediasource      STRING         COMMENT "推荐位",
planid                    STRING         COMMENT "",
versionid                 STRING         COMMENT "",
add_time                  STRING         COMMENT "时间"
)
COMMENT "商品点击数"
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\u0001' LINES TERMINATED BY '\n' STORED AS TEXTFILE;


--商品加购数	
CREATE TABLE IF NOT EXISTS dw_zaful_recommend.rewrite_zaful_app_abtest_cart_num_tmp(
cart_num                 INT            COMMENT "商品加购数",
platform                  STRING         COMMENT "android或者ios",
af_inner_mediasource      STRING         COMMENT "推荐位",
planid                    STRING         COMMENT "",
versionid                 STRING         COMMENT "",
add_time                  STRING         COMMENT "时间"
)
COMMENT "商品加购数"
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\u0001' LINES TERMINATED BY '\n' STORED AS TEXTFILE;







--商品收藏数	
CREATE TABLE IF NOT EXISTS dw_zaful_recommend.rewrite_zaful_app_abtest_collect_num_tmp(
collect_num                 INT            COMMENT "商品收藏数",
platform                  STRING         COMMENT "android或者ios",
af_inner_mediasource      STRING         COMMENT "推荐位",
planid                    STRING         COMMENT "",
versionid                 STRING         COMMENT "",
add_time                  STRING         COMMENT "时间"
)
COMMENT "商品收藏数"
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\u0001' LINES TERMINATED BY '\n' STORED AS TEXTFILE;


--中间表rewrite_zaful_app_abtest_sku_user_country_tmp：取sku,user_id
CREATE TABLE IF NOT EXISTS dw_zaful_recommend.rewrite_zaful_app_abtest_sku_user_country_tmp(
goods_sn                  STRING         COMMENT "sku",
user_id                   STRING         COMMENT "用户ID",
platform                  STRING         COMMENT "平台",
af_inner_mediasource      STRING         COMMENT "推荐位编号",
planid                    STRING         COMMENT "",
versionid                 STRING         COMMENT ""
)
COMMENT "推荐位报表user-sku-county中间表"
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\u0001' LINES TERMINATED BY '\n' STORED AS TEXTFILE;

--订单数	
CREATE TABLE IF NOT EXISTS dw_zaful_recommend.rewrite_zaful_app_abtest_order_num_tmp(
order_num                 INT            COMMENT "订单数",
platform                  STRING         COMMENT "android或者ios",
af_inner_mediasource      STRING         COMMENT "推荐位",
planid                    STRING         COMMENT "",
versionid                 STRING         COMMENT "",
add_time                  STRING         COMMENT "时间"
)
COMMENT "订单数"
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\u0001' LINES TERMINATED BY '\n' STORED AS TEXTFILE;


--订单金额	
CREATE TABLE IF NOT EXISTS dw_zaful_recommend.rewrite_zaful_app_abtest_order_amount_tmp(
order_amount                 INT            COMMENT "订单金额",
platform                  STRING         COMMENT "android或者ios",
af_inner_mediasource      STRING         COMMENT "推荐位",
planid                    STRING         COMMENT "",
versionid                 STRING         COMMENT "",
add_time                  STRING         COMMENT "时间"
)
COMMENT "订单金额"
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\u0001' LINES TERMINATED BY '\n' STORED AS TEXTFILE;

--gmv	
CREATE TABLE IF NOT EXISTS dw_zaful_recommend.rewrite_zaful_app_abtest_order_gmv_tmp(
order_gmv                 INT            COMMENT "gmv",
platform                  STRING         COMMENT "android或者ios",
af_inner_mediasource      STRING         COMMENT "推荐位",
planid                    STRING         COMMENT "",
versionid                 STRING         COMMENT "",
add_time                  STRING         COMMENT "时间"
)
COMMENT "订单金额"
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\u0001' LINES TERMINATED BY '\n' STORED AS TEXTFILE;



--商品曝光数
INSERT OVERWRITE TABLE dw_zaful_recommend.rewrite_zaful_app_abtest_exp_num_tmp
SELECT
sum(m.exp_num) as exp_num,
m.platform,
m.af_inner_mediasource,
m.planid,
m.versionid,
'${ADD_TIME}' as add_time
FROM
(
  SELECT
  platform,
  get_json_object(event_value, '$.af_inner_mediasource') as af_inner_mediasource,
  length(get_json_object(event_value, '$.af_content_id'))-length(regexp_replace(get_json_object(event_value, '$.af_content_id'),',',''))+1 as exp_num,
       concat_ws('-', year, month, day)  as add_time,
     get_json_object(event_value, '$.af_plan_id') as planid,
	 get_json_object(event_value, '$.af_version_id') as versionid
 
  FROM
ods.ods_app_burial_log 
  WHERE
  concat_ws('-', year, month, day)  = '${ADD_TIME}'
  and event_name='af_impression'
  and site='zaful'
  and get_json_object(event_value, '$.af_inner_mediasource') in ('recommend_homepage','recommend productdetail')
) m 
group by
m.platform,
m.af_inner_mediasource,
m.planid,
m.versionid
;

--商品点击数
INSERT OVERWRITE TABLE dw_zaful_recommend.rewrite_zaful_app_abtest_click_num_tmp
SELECT   count(*) AS click_num,
m.platform,
m.af_inner_mediasource,
m.planid,
m.versionid,
'${ADD_TIME}' as add_time
from (
SELECT

  platform,
  get_json_object(event_value, '$.af_inner_mediasource') as af_inner_mediasource,
  concat_ws('-', year, month, day) as add_time,
     get_json_object(event_value, '$.af_plan_id') as planid,
	 get_json_object(event_value, '$.af_version_id') as versionid
FROM
ods.ods_app_burial_log 
WHERE
  concat_ws('-', year, month, day) = '${ADD_TIME}'
  AND event_name='af_view_product'
  AND get_json_object(event_value, '$.af_changed_size_or_color') = 0
      and site='zaful'
  and get_json_object(event_value, '$.af_inner_mediasource') in ('recommend_homepage','recommend productdetail')
) m
group by
m.platform,
m.af_inner_mediasource,
m.planid,
m.versionid
;



--商品加购数
INSERT OVERWRITE TABLE dw_zaful_recommend.rewrite_zaful_app_abtest_cart_num_tmp
SELECT
sum(m.cart_num) as cart_num,
m.platform,
m.af_inner_mediasource,
m.planid,
m.versionid,
'${ADD_TIME}' as add_time
FROM
(
  SELECT
  platform,
  get_json_object(event_value, '$.af_inner_mediasource') as af_inner_mediasource,
  get_json_object(event_value, '$.af_quantity') as cart_num,
   concat_ws('-', year, month, day) as add_time,
     get_json_object(event_value, '$.af_plan_id') as planid,
	 get_json_object(event_value, '$.af_version_id') as versionid
  FROM
ods.ods_app_burial_log 
  WHERE
  concat_ws('-', year, month, day) = '${ADD_TIME}'
  and event_name='af_add_to_bag'
      and site='zaful'
  and get_json_object(event_value, '$.af_inner_mediasource') in ('recommend_homepage','recommend productdetail')
) m 
group by
m.platform,
m.af_inner_mediasource,
m.planid,
m.versionid
;


--商品收藏数	
INSERT OVERWRITE TABLE dw_zaful_recommend.rewrite_zaful_app_abtest_collect_num_tmp
SELECT   count(*) as collect_num,
m.platform,
m.af_inner_mediasource,
m.planid,
m.versionid,
'${ADD_TIME}' as add_time
from (
SELECT
  platform,
  get_json_object(event_value, '$.af_inner_mediasource') AS af_inner_mediasource,
   concat_ws('-', year, month, day) as add_time,
     get_json_object(event_value, '$.af_plan_id') as planid,
	 get_json_object(event_value, '$.af_version_id') as versionid
   FROM
ods.ods_app_burial_log 
  WHERE
  concat_ws('-', year, month, day)  ='${ADD_TIME}'
  and event_name='af_add_to_wishlist'
      and site='zaful'
  and get_json_object(event_value, '$.af_inner_mediasource') in ('recommend_homepage','recommend productdetail')
) m
group by
m.platform,
m.af_inner_mediasource,
m.planid,
m.versionid
;


--========================================================================================

--device_id,user对应表
INSERT OVERWRITE TABLE  dw_zaful_recommend.zaful_app_u_map
SELECT
m.appsflyer_device_id,
m.customer_user_id
FROM 
(
  SELECT
  appsflyer_device_id,
  customer_user_id
  FROM
  dw_zaful_recommend.zaful_app_u_map
  union all
  SELECT
  appsflyer_device_id,
  customer_user_id
  FROM
  ods.ods_app_burial_log 
  where 
  concat_ws('-', year, month, day) = '${ADD_TIME}'
      AND customer_user_id <> '0'
      AND customer_user_id <> ''
      and site='zaful'
) m
GROUP BY
m.appsflyer_device_id,
m.customer_user_id 
;

--中间表report_sku_user_tmp：取sku,user_id
INSERT OVERWRITE TABLE dw_zaful_recommend.rewrite_zaful_app_abtest_sku_user_country_tmp
SELECT
  m.goods_sn,
  n.customer_user_id,
  m.platform,
  m.af_inner_mediasource,
  m.planid,
  m.versionid
FROM
  (
    SELECT
      appsflyer_device_id,
      --regexp_extract(event_value, '(.*?sku":")([0-9a-zA-Z]*)(".*?)', 2) AS sku,
      get_json_object(event_value, '$.af_content_id') AS goods_sn,
      platform,
      get_json_object(event_value, '$.af_inner_mediasource')  AS af_inner_mediasource,
	  get_json_object(event_value, '$.af_plan_id') as planid,
	  get_json_object(event_value, '$.af_version_id') as versionid
    FROM
      ods.ods_app_burial_log 
    WHERE
      concat_ws('-', year, month, day) BETWEEN '${ADD_TIME_W}'
      AND '${ADD_TIME}'
      AND event_name='af_add_to_bag'
      and site='zaful'
      AND get_json_object(event_value, '$.af_content_id') is not null 
  ) m
  INNER JOIN dw_zaful_recommend.zaful_app_u_map n ON m.appsflyer_device_id = n.appsflyer_device_id
GROUP BY
  m.goods_sn,
  n.customer_user_id,
  m.platform,
  m.af_inner_mediasource,
  m.planid,
  m.versionid
;


--中间表
CREATE TABLE IF NOT EXISTS dw_zaful_recommend.rewrite_app_report_sku_user_tmp_orderid(
goods_sn                  STRING         COMMENT "sku",
user_id                   STRING         COMMENT "用户ID",
order_status              STRING         COMMENT "订单状态",
pay_amount                decimal(10,2)  COMMENT "购买金额",
goods_number              STRING         COMMENT "商品数量",
order_id                  STRING         COMMENT ""
)
COMMENT "推荐位报表user-sku中间表"
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\u0001' LINES TERMINATED BY '\n' STORED AS TEXTFILE;


--中间表report_sku_user_tmp：取sku,user_id
INSERT OVERWRITE TABLE dw_zaful_recommend.rewrite_app_report_sku_user_tmp_orderid
SELECT
  p.goods_sn,
  x.user_id,
  x.order_status,
  p.pay_amount,
  p.goods_number,
  p.order_id
FROM
  (
    SELECT
      order_id,
      user_id,
      add_time,
      order_status
    FROM
      stg.zaful_eload_order_info
    WHERE
      from_unixtime(add_time+8*3600, 'yyyy-MM-dd') = '${ADD_TIME}'
  ) x
  JOIN
  (
  SELECT goods_sn,order_id,
  goods_number,
  case when goods_pay_amount <> '0' then goods_pay_amount
  else goods_price*goods_number end as pay_amount
  from
   stg.zaful_eload_order_goods ) p
   ON x.order_id = p.order_id
;



--订单量
INSERT OVERWRITE TABLE dw_zaful_recommend.rewrite_zaful_app_abtest_order_num_tmp
SELECT
  count(distinct x2.order_id) AS order_num,
  x1.platform,
  x1.af_inner_mediasource,
  x1.planid,
  x1.versionid,
  '${ADD_TIME}' as add_time
from
  dw_zaful_recommend.rewrite_zaful_app_abtest_sku_user_country_tmp x1
  INNER JOIN dw_zaful_recommend.rewrite_app_report_sku_user_tmp_orderid x2 ON x1.user_id = x2.user_id
  AND x1.goods_sn = x2.goods_sn
where
  x2.order_status not in ('0', '11')
group by
  x1.platform,
  x1.af_inner_mediasource,
  x1.planid,
  x1.versionid
;



--销售额
INSERT OVERWRITE TABLE dw_zaful_recommend.rewrite_zaful_app_abtest_order_amount_tmp
SELECT
  sum(x2.pay_amount) as pay_amount,
  x1.platform,
  x1.af_inner_mediasource,
  x1.planid,
  x1.versionid,
  '${ADD_TIME}' as add_time
from
  dw_zaful_recommend.rewrite_zaful_app_abtest_sku_user_country_tmp x1
  INNER JOIN dw_zaful_recommend.rewrite_app_report_sku_user_tmp_orderid x2 ON x1.user_id = x2.user_id
  AND x1.goods_sn = x2.goods_sn
where
  x2.order_status not in ('0', '11')
group by
  x1.platform,
  x1.af_inner_mediasource,
  x1.planid,
  x1.versionid
  ;


--GMV
INSERT OVERWRITE TABLE dw_zaful_recommend.rewrite_zaful_app_abtest_order_gmv_tmp
SELECT
  sum(x2.pay_amount) as order_gmv,
  x1.platform,
  x1.af_inner_mediasource,
  x1.planid,
  x1.versionid,
  '${ADD_TIME}' as add_time
from
  dw_zaful_recommend.rewrite_zaful_app_abtest_sku_user_country_tmp x1
  INNER JOIN dw_zaful_recommend.rewrite_app_report_sku_user_tmp_orderid x2 ON x1.user_id = x2.user_id
  AND x1.goods_sn = x2.goods_sn
group by
  x1.platform,
  x1.af_inner_mediasource,
  x1.planid,
  x1.versionid
  ;



--所有结果汇总
INSERT into TABLE dw_zaful_recommend.rewrite_zaful_app_abtest_report PARTITION (add_time = '${ADD_TIME}')
select
  a.exp_num,
  b.click_num,
  c.cart_num,
  d.collect_num,
  e.order_num,
  f.order_amount,
  g.order_gmv,
  'android',
  'recommend productdetail',
  a.planid,
  a.versionid
from 
    (select exp_num,platform,af_inner_mediasource,planid,versionid,add_time from dw_zaful_recommend.rewrite_zaful_app_abtest_exp_num_tmp where af_inner_mediasource='recommend productdetail' and platform='android' ) a
    join
    (select click_num,platform,af_inner_mediasource,planid,versionid,add_time from dw_zaful_recommend.rewrite_zaful_app_abtest_click_num_tmp where af_inner_mediasource='recommend productdetail' and platform='android' ) b
    on a.add_time=b.add_time and a.planid=b.planid and a.versionid=b.versionid
	join 
	(select cart_num,platform,af_inner_mediasource,planid,versionid,add_time from dw_zaful_recommend.rewrite_zaful_app_abtest_cart_num_tmp where af_inner_mediasource='recommend productdetail' and platform='android' ) c
    on a.add_time=c.add_time and a.planid=c.planid and a.versionid=c.versionid
	join
	(select collect_num,platform,af_inner_mediasource,planid,versionid,add_time from dw_zaful_recommend.rewrite_zaful_app_abtest_collect_num_tmp where af_inner_mediasource='recommend productdetail' and platform='android' ) d
    on a.add_time=d.add_time and a.planid=d.planid and a.versionid=d.versionid
	join
	(select order_num,platform,af_inner_mediasource,planid,versionid,add_time from dw_zaful_recommend.rewrite_zaful_app_abtest_order_num_tmp where af_inner_mediasource='recommend productdetail' and platform='android' ) e
    on a.add_time=e.add_time and a.planid=e.planid and a.versionid=e.versionid
	join
	(select order_amount,platform,af_inner_mediasource,planid,versionid,add_time from dw_zaful_recommend.rewrite_zaful_app_abtest_order_amount_tmp where af_inner_mediasource='recommend productdetail' and platform='android' ) f
    on a.add_time=f.add_time and a.planid=f.planid and a.versionid=f.versionid
  join
  (select order_gmv,platform,af_inner_mediasource,planid,versionid,add_time from dw_zaful_recommend.rewrite_zaful_app_abtest_order_gmv_tmp where af_inner_mediasource='recommend productdetail' and platform='android' ) g
    on a.add_time=g.add_time and a.planid=g.planid and a.versionid=g.versionid
union all
select
  a.exp_num,
  b.click_num,
  c.cart_num,
  d.collect_num,
  e.order_num,
  f.order_amount,
  g.order_gmv,
  'ios',
  'recommend productdetail',
  a.planid,
  a.versionid
from 
    (select exp_num,platform,af_inner_mediasource,planid,versionid,add_time from dw_zaful_recommend.rewrite_zaful_app_abtest_exp_num_tmp where af_inner_mediasource='recommend productdetail' and platform='ios' ) a
    join
    (select click_num,platform,af_inner_mediasource,planid,versionid,add_time from dw_zaful_recommend.rewrite_zaful_app_abtest_click_num_tmp where af_inner_mediasource='recommend productdetail' and platform='ios' ) b
    on a.add_time=b.add_time and a.planid=b.planid and a.versionid=b.versionid
	join 
	(select cart_num,platform,af_inner_mediasource,planid,versionid,add_time from dw_zaful_recommend.rewrite_zaful_app_abtest_cart_num_tmp where af_inner_mediasource='recommend productdetail' and platform='ios' ) c
    on a.add_time=c.add_time and a.planid=c.planid and a.versionid=c.versionid
	join
	(select collect_num,platform,af_inner_mediasource,planid,versionid,add_time from dw_zaful_recommend.rewrite_zaful_app_abtest_collect_num_tmp where af_inner_mediasource='recommend productdetail' and platform='ios' ) d
    on a.add_time=d.add_time and a.planid=d.planid and a.versionid=d.versionid
	join
	(select order_num,platform,af_inner_mediasource,planid,versionid,add_time from dw_zaful_recommend.rewrite_zaful_app_abtest_order_num_tmp where af_inner_mediasource='recommend productdetail' and platform='ios' ) e
    on a.add_time=e.add_time and a.planid=e.planid and a.versionid=e.versionid
	join
	(select order_amount,platform,af_inner_mediasource,planid,versionid,add_time from dw_zaful_recommend.rewrite_zaful_app_abtest_order_amount_tmp where af_inner_mediasource='recommend productdetail' and platform='ios' ) f
    on a.add_time=f.add_time and a.planid=f.planid and a.versionid=f.versionid
    join
  (select order_gmv,platform,af_inner_mediasource,planid,versionid,add_time from dw_zaful_recommend.rewrite_zaful_app_abtest_order_gmv_tmp where af_inner_mediasource='recommend productdetail' and platform='ios' ) g
    on a.add_time=g.add_time and a.planid=g.planid and a.versionid=g.versionid
union all
select
  a.exp_num,
  b.click_num,
  c.cart_num,
  d.collect_num,
  e.order_num,
  f.order_amount,
  g.order_gmv,
  'android',
  'recommend_homepage',
  a.planid,
  a.versionid
from 
    (select exp_num,platform,af_inner_mediasource,planid,versionid,add_time from dw_zaful_recommend.rewrite_zaful_app_abtest_exp_num_tmp where af_inner_mediasource='recommend_homepage' and platform='android' ) a
    join
    (select click_num,platform,af_inner_mediasource,planid,versionid,add_time from dw_zaful_recommend.rewrite_zaful_app_abtest_click_num_tmp where af_inner_mediasource='recommend_homepage' and platform='android' ) b
    on a.add_time=b.add_time and a.planid=b.planid and a.versionid=b.versionid
	join 
	(select cart_num,platform,af_inner_mediasource,planid,versionid,add_time from dw_zaful_recommend.rewrite_zaful_app_abtest_cart_num_tmp where af_inner_mediasource='recommend_homepage' and platform='android' ) c
    on a.add_time=c.add_time and a.planid=c.planid and a.versionid=c.versionid
	join
	(select collect_num,platform,af_inner_mediasource,planid,versionid,add_time from dw_zaful_recommend.rewrite_zaful_app_abtest_collect_num_tmp where af_inner_mediasource='recommend_homepage' and platform='android' ) d
    on a.add_time=d.add_time and a.planid=d.planid and a.versionid=d.versionid
	join
	(select order_num,platform,af_inner_mediasource,planid,versionid,add_time from dw_zaful_recommend.rewrite_zaful_app_abtest_order_num_tmp where af_inner_mediasource='recommend_homepage' and platform='android' ) e
    on a.add_time=e.add_time and a.planid=e.planid and a.versionid=e.versionid
	join
	(select order_amount,platform,af_inner_mediasource,planid,versionid,add_time from dw_zaful_recommend.rewrite_zaful_app_abtest_order_amount_tmp where af_inner_mediasource='recommend_homepage' and platform='android' ) f
    on a.add_time=f.add_time and a.planid=f.planid and a.versionid=f.versionid
    join
  (select order_gmv,platform,af_inner_mediasource,planid,versionid,add_time from dw_zaful_recommend.rewrite_zaful_app_abtest_order_gmv_tmp where af_inner_mediasource='recommend_homepage' and platform='android' ) g
    on a.add_time=g.add_time and a.planid=g.planid and a.versionid=g.versionid
union all
select
  a.exp_num,
  b.click_num,
  c.cart_num,
  d.collect_num,
  e.order_num,
  f.order_amount,
  g.order_gmv,
  'ios',
  'recommend_homepage',
  a.planid,
  a.versionid
from 
    (select exp_num,platform,af_inner_mediasource,planid,versionid,add_time from dw_zaful_recommend.rewrite_zaful_app_abtest_exp_num_tmp where af_inner_mediasource='recommend_homepage' and platform='ios' ) a
    join
    (select click_num,platform,af_inner_mediasource,planid,versionid,add_time from dw_zaful_recommend.rewrite_zaful_app_abtest_click_num_tmp where af_inner_mediasource='recommend_homepage' and platform='ios' ) b
    on a.add_time=b.add_time and a.planid=b.planid and a.versionid=b.versionid
	join 
	(select cart_num,platform,af_inner_mediasource,planid,versionid,add_time from dw_zaful_recommend.rewrite_zaful_app_abtest_cart_num_tmp where af_inner_mediasource='recommend_homepage' and platform='ios' ) c
    on a.add_time=c.add_time and a.planid=c.planid and a.versionid=c.versionid
	join
	(select collect_num,platform,af_inner_mediasource,planid,versionid,add_time from dw_zaful_recommend.rewrite_zaful_app_abtest_collect_num_tmp where af_inner_mediasource='recommend_homepage' and platform='ios' ) d
    on a.add_time=d.add_time and a.planid=d.planid and a.versionid=d.versionid
	join
	(select order_num,platform,af_inner_mediasource,planid,versionid,add_time from dw_zaful_recommend.rewrite_zaful_app_abtest_order_num_tmp where af_inner_mediasource='recommend_homepage' and platform='ios' ) e
    on a.add_time=e.add_time and a.planid=e.planid and a.versionid=e.versionid
	join
	(select order_amount,platform,af_inner_mediasource,planid,versionid,add_time from dw_zaful_recommend.rewrite_zaful_app_abtest_order_amount_tmp where af_inner_mediasource='recommend_homepage' and platform='ios' ) f
    on a.add_time=f.add_time and a.planid=f.planid and a.versionid=f.versionid
    join
  (select order_gmv,platform,af_inner_mediasource,planid,versionid,add_time from dw_zaful_recommend.rewrite_zaful_app_abtest_order_gmv_tmp where af_inner_mediasource='recommend_homepage' and platform='ios' ) g
    on a.add_time=g.add_time and a.planid=g.planid and a.versionid=g.versionid
