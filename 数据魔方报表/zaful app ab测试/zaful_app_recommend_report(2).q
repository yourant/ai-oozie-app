
--@author ZhanRui
--@date 2018年10月08日 
--@desc  Zaful App推荐位报表

SET mapred.job.name=zaful_app_recommend_report;
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
CREATE TABLE IF NOT EXISTS dw_zaful_recommend.zaful_app_recommend_report(
pv                        INT            COMMENT "页面PV",
uv                        INT            COMMENT "页面UV",
exp_num                   INT            COMMENT "商品曝光数",
click_num                 INT            COMMENT "商品点击数",
exp_click_ratio           decimal(10,4)  COMMENT "曝光点击率",
cart_num                  INT            COMMENT "商品加购数",
cart_ratio                decimal(10,4)  COMMENT "加购率",
order_num                 INT            COMMENT "商品下单数",   
conversion_ratio          decimal(10,4)  COMMENT "下单转化率",
purchase_num              INT            COMMENT "支付订单数",  
pay_amount                decimal(10,4)  COMMENT "购买金额",
purchase_ratio            decimal(10,4)  COMMENT "购买转化率",
total_conversion_ratio    decimal(10,4)  COMMENT "总体转化率",
collect_num               INT            COMMENT "商品收藏数",
platform                  STRING         COMMENT "平台",
recommend_position        STRING         COMMENT "推荐位编号",
position_name             STRING         COMMENT "推荐位名称"
--glb_dc                    STRING         COMMENT "语言站",
--country                   STRING         COMMENT "国家"
)
COMMENT 'APP推荐位数据报表'
PARTITIONED BY (add_time STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\u0001' LINES TERMINATED BY '\n' STORED AS TEXTFILE
;


--页面PV 
CREATE TABLE IF NOT EXISTS dw_zaful_recommend.app_report_pv_tmp(
pv                        INT            COMMENT "页面PV",
platform                  STRING         COMMENT "平台",
af_inner_mediasource      STRING         COMMENT "推荐位",
--country                   STRING         COMMENT "国家",
--glb_dc                    STRING         COMMENT "语言站",
add_time                  STRING         COMMENT "日期"
)
COMMENT "推荐位报表页面PV中间表"
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\u0001' LINES TERMINATED BY '\n' STORED AS TEXTFILE;

--页面UV	
CREATE TABLE IF NOT EXISTS dw_zaful_recommend.app_report_uv_tmp(
uv                        INT            COMMENT "页面UV",
platform                  STRING         COMMENT "平台",
af_inner_mediasource      STRING         COMMENT "推荐位",
--country                   STRING         COMMENT "国家",
--glb_dc                    STRING         COMMENT "语言站",
add_time                  STRING         COMMENT "日期"
)
COMMENT "推荐位报表页面UV中间表"
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\u0001' LINES TERMINATED BY '\n' STORED AS TEXTFILE;

--商品曝光数	
CREATE TABLE IF NOT EXISTS dw_zaful_recommend.app_report_exp_tmp(
exp_num                   INT            COMMENT "商品曝光数",
platform                  STRING         COMMENT "平台",
af_inner_mediasource      STRING         COMMENT "推荐位",
--country                   STRING         COMMENT "国家",
--glb_dc                    STRING         COMMENT "语言站",
add_time                  STRING         COMMENT "日期"
)
COMMENT "推荐位报表商品曝光数中间表"
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\u0001' LINES TERMINATED BY '\n' STORED AS TEXTFILE;

--商品点击数	
CREATE TABLE IF NOT EXISTS dw_zaful_recommend.app_report_click_tmp(
click_num                 INT            COMMENT "商品点击数",
platform                  STRING         COMMENT "平台",
af_inner_mediasource      STRING         COMMENT "推荐位",
--country                   STRING         COMMENT "国家",
--glb_dc                    STRING         COMMENT "语言站",
add_time                  STRING         COMMENT "日期"
)
COMMENT "推荐位报表商品点击数中间表"
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\u0001' LINES TERMINATED BY '\n' STORED AS TEXTFILE;

--商品加购数	
CREATE TABLE IF NOT EXISTS dw_zaful_recommend.app_report_cart_tmp(
cart_num                  INT            COMMENT "商品加购数",
platform                  STRING         COMMENT "平台",
af_inner_mediasource      STRING         COMMENT "推荐位",
--country                   STRING         COMMENT "国家",
--glb_dc                    STRING         COMMENT "语言站",
add_time                  STRING         COMMENT "日期"
)
COMMENT "推荐位报表商品加购数中间表"
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\u0001' LINES TERMINATED BY '\n' STORED AS TEXTFILE;

--下单商品数	

--中间表report_sku_user_tmp：取sku,user_id
CREATE TABLE IF NOT EXISTS dw_zaful_recommend.app_report_sku_user_country_tmp(
goods_sn                  STRING         COMMENT "sku",
user_id                   STRING         COMMENT "用户ID",
platform                  STRING         COMMENT "平台",
--country                   STRING         COMMENT "国家",
--glb_dc                    STRING         COMMENT "语言站",
af_inner_mediasource      STRING         COMMENT "推荐位编号"
)
COMMENT "推荐位报表user-sku-county中间表"
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\u0001' LINES TERMINATED BY '\n' STORED AS TEXTFILE;

CREATE TABLE IF NOT EXISTS dw_zaful_recommend.app_report_sku_user_tmp(
goods_sn                  STRING         COMMENT "sku",
user_id                   STRING         COMMENT "用户ID",
order_status              STRING         COMMENT "订单状态",
pay_amount                decimal(10,2)  COMMENT "购买金额",
goods_number              STRING         COMMENT "商品数量"
)
COMMENT "推荐位报表user-sku中间表"
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\u0001' LINES TERMINATED BY '\n' STORED AS TEXTFILE;

--下单商品数结果表
CREATE TABLE IF NOT EXISTS dw_zaful_recommend.app_report_order_tmp(
order_num                 INT            COMMENT "商品下单数",
platform                  STRING         COMMENT "平台",
af_inner_mediasource      STRING         COMMENT "推荐位",
--country                   STRING         COMMENT "国家",
--glb_dc                    STRING         COMMENT "语言站",
add_time                  STRING         COMMENT "日期"
)
COMMENT "推荐位报表商品下单数中间表"
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\u0001' LINES TERMINATED BY '\n' STORED AS TEXTFILE;

--销量
CREATE TABLE IF NOT EXISTS dw_zaful_recommend.app_report_purchase_tmp(
purchase_num              INT            COMMENT "支付订单数",
platform                  STRING         COMMENT "平台",
af_inner_mediasource      STRING         COMMENT "推荐位",
--country                   STRING         COMMENT "国家",
--glb_dc                    STRING         COMMENT "语言站",
add_time                  STRING         COMMENT "日期"
)
COMMENT "推荐位报表支付订单数中间表"
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\u0001' LINES TERMINATED BY '\n' STORED AS TEXTFILE;

--销售额
CREATE TABLE IF NOT EXISTS dw_zaful_recommend.app_report_pay_amount_tmp(
pay_amount                decimal(10,2)  COMMENT "购买金额",
platform                  STRING         COMMENT "平台",
af_inner_mediasource      STRING         COMMENT "推荐位",
--country                   STRING         COMMENT "国家",
--glb_dc                    STRING         COMMENT "语言站",
add_time                  STRING         COMMENT "日期"
)
COMMENT "推荐位报表购买金额中间表"
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\u0001' LINES TERMINATED BY '\n' STORED AS TEXTFILE;

--商品收藏数	
CREATE TABLE IF NOT EXISTS dw_zaful_recommend.app_report_collect_tmp(
collect_num               INT            COMMENT "商品收藏数",
platform                  STRING         COMMENT "平台",
af_inner_mediasource      STRING         COMMENT "推荐位",
--country                   STRING         COMMENT "国家",
--glb_dc                    STRING         COMMENT "语言站",
add_time                  STRING         COMMENT "日期"
)
COMMENT "推荐位报表商品收藏数中间表"
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\u0001' LINES TERMINATED BY '\n' STORED AS TEXTFILE;

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
INSERT OVERWRITE TABLE dw_zaful_recommend.app_report_sku_user_country_tmp
SELECT
  m.goods_sn,
  n.customer_user_id,
  m.platform,
  m.af_inner_mediasource
FROM
  (
    SELECT
      appsflyer_device_id,
      --regexp_extract(event_value, '(.*?sku":")([0-9a-zA-Z]*)(".*?)', 2) AS sku,
      get_json_object(event_value, '$.af_content_id') AS goods_sn,
      platform,
      get_json_object(event_value, '$.af_inner_mediasource') AS af_inner_mediasource
    FROM
      ods.ods_app_burial_log 
    WHERE
      concat_ws('-', year, month, day) BETWEEN '${ADD_TIME_W}'
      AND '${ADD_TIME}'
      AND event_name='af_add_to_bag'
      and app_version =	'3.8.0'
      and site='zaful'
      AND get_json_object(event_value, '$.af_content_id') is not null 
  ) m
  INNER JOIN dw_zaful_recommend.zaful_app_u_map n ON m.appsflyer_device_id = n.appsflyer_device_id
GROUP BY
  m.goods_sn,
  n.customer_user_id,
  m.platform,
  m.af_inner_mediasource
;

--中间表report_sku_user_tmp：取sku,user_id
INSERT OVERWRITE TABLE dw_zaful_recommend.app_report_sku_user_tmp
SELECT
  p.goods_sn,
  x.user_id,
  x.order_status,
  p.pay_amount,
  p.goods_number
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




--页面PV
INSERT OVERWRITE TABLE dw_zaful_recommend.app_report_pv_tmp
SELECT
  count(*) as pv,
  platform,
  'recommend_homepage' as af_inner_mediasource,
  '${ADD_TIME}' as add_time
FROM
  ods.ods_app_burial_log 
WHERE
  concat_ws('-', year, month, day) = '${ADD_TIME}'
  and event_name='af_view_homepage'
  and get_json_object(event_value, '$.af_content_type') ='view homepage'
  and upper(get_json_object(event_value, '$.af_channel_name')) ='TRENDS'
  AND app_version='3.8.0'
  and site='zaful'
group by
  platform
union all 
SELECT
  count(*) as pv,
  platform,
  'recommend productdetail' as af_inner_mediasource,
  '${ADD_TIME}' as add_time
FROM
  ods.ods_app_burial_log 
WHERE
  concat_ws('-', year, month, day) = '${ADD_TIME}'
  and event_name='af_view_product'
  AND app_version='3.8.0'
  and site='zaful'
group by
  platform
;

--页面UV
INSERT OVERWRITE TABLE dw_zaful_recommend.app_report_uv_tmp
SELECT
  count(*) AS uv,
  m.platform,
  m.af_inner_mediasource,
  '${ADD_TIME}' as add_time
FROM
  (
    SELECT
      appsflyer_device_id,
      platform,
      'recommend_homepage' as af_inner_mediasource
    FROM
      ods.ods_app_burial_log 
    WHERE
      concat_ws('-', year, month, day) = '${ADD_TIME}'
        and event_name='af_view_homepage'
        and get_json_object(event_value, '$.af_content_type') ='view homepage'
        and upper(get_json_object(event_value, '$.af_channel_name')) ='TRENDS'
        AND app_version='3.8.0'
        and site='zaful'
    GROUP BY
      appsflyer_device_id,
      platform
    union all 
    SELECT
      appsflyer_device_id,
      platform,
      'recommend productdetail' as af_inner_mediasource
    FROM
      ods.ods_app_burial_log 
    WHERE
      concat_ws('-', year, month, day) = '${ADD_TIME}'
      and event_name='af_view_product'
      AND app_version='3.8.0'
      and site='zaful'
    group by
      appsflyer_device_id,
      platform
  ) m
group by
  m.platform,
  m.af_inner_mediasource
  ;




--商品曝光数
INSERT OVERWRITE TABLE dw_zaful_recommend.app_report_exp_tmp
SELECT
sum(m.exp_num) as exp_num,
m.platform,
m.af_inner_mediasource,
'${ADD_TIME}' as add_time
FROM
(
  SELECT
  platform,
  get_json_object(event_value, '$.af_inner_mediasource') as af_inner_mediasource,
  length(get_json_object(event_value, '$.af_content_id'))-length(regexp_replace(get_json_object(event_value, '$.af_content_id'),',',''))+1 as exp_num
  FROM
  ods.ods_app_burial_log 
  WHERE
  concat_ws('-', year, month, day) = '${ADD_TIME}'
  and event_name='af_impression'
  and get_json_object(event_value, '$.af_inner_mediasource') in ('recommend_homepage','recommend productdetail')
  AND app_version='3.8.0'
      and site='zaful'
) m 
group by
m.platform,
m.af_inner_mediasource
;

--商品点击数
INSERT OVERWRITE TABLE dw_zaful_recommend.app_report_click_tmp
SELECT
  count(*) AS click_num,
  platform,
  get_json_object(event_value, '$.af_inner_mediasource') as af_inner_mediasource,
  '${ADD_TIME}' as add_time
FROM
  ods.ods_app_burial_log 
WHERE
  concat_ws('-', year, month, day) = '${ADD_TIME}'
  AND event_name='af_view_product'
  AND get_json_object(event_value, '$.af_changed_size_or_color') = 0
  and get_json_object(event_value, '$.af_inner_mediasource') in ('recommend_homepage','recommend productdetail')
  AND app_version='3.8.0'
      and site='zaful'
group by
  platform,
  get_json_object(event_value, '$.af_inner_mediasource')
;


--商品加购数
INSERT OVERWRITE TABLE dw_zaful_recommend.app_report_cart_tmp
SELECT
sum(m.cart_num) as cart_num,
m.platform,
m.af_inner_mediasource,
'${ADD_TIME}' as add_time
FROM
(
  SELECT
  platform,
  get_json_object(event_value, '$.af_inner_mediasource') as af_inner_mediasource,
  get_json_object(event_value, '$.af_quantity') as cart_num
  FROM
  ods.ods_app_burial_log 
  WHERE
  concat_ws('-', year, month, day) = '${ADD_TIME}'
  and event_name='af_add_to_bag'
  and get_json_object(event_value, '$.af_inner_mediasource') in ('recommend_homepage','recommend productdetail')
  AND app_version='3.8.0'
      and site='zaful'
) m 
group by
m.platform,
m.af_inner_mediasource
;

--下单商品数
INSERT OVERWRITE TABLE dw_zaful_recommend.app_report_order_tmp
SELECT
  SUM(x2.goods_number) AS order_num,
  x1.platform,
  x1.af_inner_mediasource,
  '${ADD_TIME}' as add_time
from
  dw_zaful_recommend.app_report_sku_user_country_tmp x1
  INNER JOIN dw_zaful_recommend.app_report_sku_user_tmp x2 ON x1.user_id = x2.user_id
  AND x1.goods_sn = x2.goods_sn
group by
  x1.platform,
  x1.af_inner_mediasource
;

--销量
INSERT OVERWRITE TABLE dw_zaful_recommend.app_report_purchase_tmp
SELECT
  SUM(x2.goods_number) AS purchase_num,
  x1.platform,
  x1.af_inner_mediasource,
  '${ADD_TIME}' as add_time
from
  dw_zaful_recommend.app_report_sku_user_country_tmp x1
  INNER JOIN dw_zaful_recommend.app_report_sku_user_tmp x2 ON x1.user_id = x2.user_id
  AND x1.goods_sn = x2.goods_sn
where
  x2.order_status not in ('0', '11')
group by
  x1.platform,
  x1.af_inner_mediasource
;


--销售额
INSERT OVERWRITE TABLE dw_zaful_recommend.app_report_pay_amount_tmp
SELECT
  sum(x2.pay_amount) as pay_amount,
  x1.platform,
  x1.af_inner_mediasource,
  '${ADD_TIME}' as add_time
from
  dw_zaful_recommend.app_report_sku_user_country_tmp x1
  INNER JOIN dw_zaful_recommend.app_report_sku_user_tmp x2 ON x1.user_id = x2.user_id
  AND x1.goods_sn = x2.goods_sn
where
  x2.order_status not in ('0', '11')
group by
  x1.platform,
  x1.af_inner_mediasource
  ;

--商品收藏数	
INSERT OVERWRITE TABLE dw_zaful_recommend.app_report_collect_tmp
SELECT
  count(*) as collect_num,
  platform,
  get_json_object(event_value, '$.af_inner_mediasource') AS af_inner_mediasource,
  '${ADD_TIME}' as add_time
   FROM
  ods.ods_app_burial_log 
  WHERE
  concat_ws('-', year, month, day) = '${ADD_TIME}'
  and event_name='af_add_to_wishlist'
  and get_json_object(event_value, '$.af_inner_mediasource') in ('recommend_homepage','recommend productdetail')
   AND app_version='3.8.0'
      and site='zaful'
group by
  platform,
  get_json_object(event_value, '$.af_inner_mediasource');




--所有结果汇总
INSERT overwrite TABLE dw_zaful_recommend.zaful_app_recommend_report PARTITION (add_time = '${ADD_TIME}')
select
  NVL(a.pv,0),
  NVL(b.uv,0),
  c.exp_num,
  d.click_num,
  d.click_num / c.exp_num,
  e.cart_num,
  e.cart_num / d.click_num,
  f.order_num,
  f.order_num / e.cart_num,
  g.purchase_num,
  h.pay_amount,
  g.purchase_num / f.order_num,
  g.purchase_num / c.exp_num,
  i.collect_num,
  'android',
  'recommend productdetail',
  '商详页推荐'
from   
(select '${ADD_TIME}' as add_time) mm 
left join 
(select pv, add_time from  dw_zaful_recommend.app_report_pv_tmp where af_inner_mediasource='recommend productdetail' and platform='android') a on mm.add_time=a.add_time
left join
(select uv, add_time from  dw_zaful_recommend.app_report_uv_tmp where af_inner_mediasource='recommend productdetail' and platform='android') b on mm.add_time=b.add_time
left join 
(select exp_num, add_time from dw_zaful_recommend.app_report_exp_tmp where af_inner_mediasource='recommend productdetail' and platform='android') c on mm.add_time=c.add_time
left join 
(select click_num, add_time from dw_zaful_recommend.app_report_click_tmp where af_inner_mediasource='recommend productdetail' and platform='android') d on mm.add_time=d.add_time
left join 
(select cart_num, add_time from dw_zaful_recommend.app_report_cart_tmp where af_inner_mediasource='recommend productdetail' and platform='android') e on mm.add_time=e.add_time
left join 
(select order_num, add_time from dw_zaful_recommend.app_report_order_tmp where af_inner_mediasource='recommend productdetail' and platform='android') f on mm.add_time=f.add_time
left join 
(select purchase_num, add_time from dw_zaful_recommend.app_report_purchase_tmp where af_inner_mediasource='recommend productdetail' and platform='android') g on mm.add_time=g.add_time
left join 
(select pay_amount, add_time from dw_zaful_recommend.app_report_pay_amount_tmp where af_inner_mediasource='recommend productdetail' and platform='android') h on mm.add_time=h.add_time
left join 
(select collect_num, add_time from dw_zaful_recommend.app_report_collect_tmp where af_inner_mediasource='recommend productdetail' and platform='android') i on mm.add_time=i.add_time
union all
select
  NVL(a.pv,0),
  NVL(b.uv,0),
  c.exp_num,
  d.click_num,
  d.click_num / c.exp_num,
  e.cart_num,
  e.cart_num / d.click_num,
  f.order_num,
  f.order_num / e.cart_num,
  g.purchase_num,
  h.pay_amount,
  g.purchase_num / f.order_num,
  g.purchase_num / c.exp_num,
  i.collect_num,
  'ios',
  'recommend_homepage',
  '首页底部推荐'
from   
(select '${ADD_TIME}' as add_time) mm 
left join 
(select pv, add_time from  dw_zaful_recommend.app_report_pv_tmp where af_inner_mediasource='recommend_homepage' and platform='ios') a on mm.add_time=a.add_time
left join
(select uv, add_time from  dw_zaful_recommend.app_report_uv_tmp where af_inner_mediasource='recommend_homepage' and platform='ios') b on mm.add_time=b.add_time
left join 
(select exp_num, add_time from dw_zaful_recommend.app_report_exp_tmp where af_inner_mediasource='recommend_homepage' and platform='ios') c on mm.add_time=c.add_time
left join 
(select click_num, add_time from dw_zaful_recommend.app_report_click_tmp where af_inner_mediasource='recommend_homepage' and platform='ios') d on mm.add_time=d.add_time
left join 
(select cart_num, add_time from dw_zaful_recommend.app_report_cart_tmp where af_inner_mediasource='recommend_homepage' and platform='ios') e on mm.add_time=e.add_time
left join 
(select order_num, add_time from dw_zaful_recommend.app_report_order_tmp where af_inner_mediasource='recommend_homepage' and platform='ios') f on mm.add_time=f.add_time
left join 
(select purchase_num, add_time from dw_zaful_recommend.app_report_purchase_tmp where af_inner_mediasource='recommend_homepage' and platform='ios') g on mm.add_time=g.add_time
left join 
(select pay_amount, add_time from dw_zaful_recommend.app_report_pay_amount_tmp where af_inner_mediasource='recommend_homepage' and platform='ios') h on mm.add_time=h.add_time
left join 
(select collect_num, add_time from dw_zaful_recommend.app_report_collect_tmp where af_inner_mediasource='recommend_homepage' and platform='ios') i on mm.add_time=i.add_time
union all
select
  NVL(a.pv,0),
  NVL(b.uv,0),
  c.exp_num,
  d.click_num,
  d.click_num / c.exp_num,
  e.cart_num,
  e.cart_num / d.click_num,
  f.order_num,
  f.order_num / e.cart_num,
  g.purchase_num,
  h.pay_amount,
  g.purchase_num / f.order_num,
  g.purchase_num / c.exp_num,
  i.collect_num,
  'ios',
  'recommend productdetail',
  '商详页推荐'
from   
(select '${ADD_TIME}' as add_time) mm 
left join 
(select pv, add_time from  dw_zaful_recommend.app_report_pv_tmp where af_inner_mediasource='recommend productdetail' and platform='ios') a on mm.add_time=a.add_time
left join
(select uv, add_time from  dw_zaful_recommend.app_report_uv_tmp where af_inner_mediasource='recommend productdetail' and platform='ios') b on mm.add_time=b.add_time
left join 
(select exp_num, add_time from dw_zaful_recommend.app_report_exp_tmp where af_inner_mediasource='recommend productdetail' and platform='ios') c on mm.add_time=c.add_time
left join 
(select click_num, add_time from dw_zaful_recommend.app_report_click_tmp where af_inner_mediasource='recommend productdetail' and platform='ios') d on mm.add_time=d.add_time
left join 
(select cart_num, add_time from dw_zaful_recommend.app_report_cart_tmp where af_inner_mediasource='recommend productdetail' and platform='ios') e on mm.add_time=e.add_time
left join 
(select order_num, add_time from dw_zaful_recommend.app_report_order_tmp where af_inner_mediasource='recommend productdetail' and platform='ios') f on mm.add_time=f.add_time
left join 
(select purchase_num, add_time from dw_zaful_recommend.app_report_purchase_tmp where af_inner_mediasource='recommend productdetail' and platform='ios') g on mm.add_time=g.add_time
left join 
(select pay_amount, add_time from dw_zaful_recommend.app_report_pay_amount_tmp where af_inner_mediasource='recommend productdetail' and platform='ios') h on mm.add_time=h.add_time
left join 
(select collect_num, add_time from dw_zaful_recommend.app_report_collect_tmp where af_inner_mediasource='recommend productdetail' and platform='ios') i on mm.add_time=i.add_time
union all
select
  NVL(a.pv,0),
  NVL(b.uv,0),
  c.exp_num,
  d.click_num,
  d.click_num / c.exp_num,
  e.cart_num,
  e.cart_num / d.click_num,
  f.order_num,
  f.order_num / e.cart_num,
  g.purchase_num,
  h.pay_amount,
  g.purchase_num / f.order_num,
  g.purchase_num / c.exp_num,
  i.collect_num,
  'android',
  'recommend_homepage',
  '首页底部推荐'
from   
(select '${ADD_TIME}' as add_time) mm 
left join 
(select pv, add_time from  dw_zaful_recommend.app_report_pv_tmp where af_inner_mediasource='recommend_homepage' and platform='android') a on mm.add_time=a.add_time
left join
(select uv, add_time from  dw_zaful_recommend.app_report_uv_tmp where af_inner_mediasource='recommend_homepage' and platform='android') b on mm.add_time=b.add_time
left join 
(select exp_num, add_time from dw_zaful_recommend.app_report_exp_tmp where af_inner_mediasource='recommend_homepage' and platform='android') c on mm.add_time=c.add_time
left join 
(select click_num, add_time from dw_zaful_recommend.app_report_click_tmp where af_inner_mediasource='recommend_homepage' and platform='android') d on mm.add_time=d.add_time
left join 
(select cart_num, add_time from dw_zaful_recommend.app_report_cart_tmp where af_inner_mediasource='recommend_homepage' and platform='android') e on mm.add_time=e.add_time
left join 
(select order_num, add_time from dw_zaful_recommend.app_report_order_tmp where af_inner_mediasource='recommend_homepage' and platform='android') f on mm.add_time=f.add_time
left join 
(select purchase_num, add_time from dw_zaful_recommend.app_report_purchase_tmp where af_inner_mediasource='recommend_homepage' and platform='android') g on mm.add_time=g.add_time
left join 
(select pay_amount, add_time from dw_zaful_recommend.app_report_pay_amount_tmp where af_inner_mediasource='recommend_homepage' and platform='android') h on mm.add_time=h.add_time
left join 
(select collect_num, add_time from dw_zaful_recommend.app_report_collect_tmp where af_inner_mediasource='recommend_homepage' and platform='android') i on mm.add_time=i.add_time
;


  

INSERT overwrite table dw_zaful_recommend.zaful_app_recommend_report_exp
select
  *
FROM
  dw_zaful_recommend.zaful_app_recommend_report
where
  add_time = '${ADD_TIME}'
;