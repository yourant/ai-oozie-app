
--@author wuchao
--@date 2018年10月16日 
--@desc  Zaful App推荐位报表新需求chongxie


SET mapred.job.name=rewrite_zaful_app_recommend_report_featured_first;
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
CREATE TABLE IF NOT EXISTS dw_zaful_recommend.rewrite_zaful_app_recommend_report_featured_first(
exp_num                   INT            COMMENT "商品曝光数",
sku_uv                    INT            COMMENT "查看商品UV",
click_num                 INT            COMMENT "商品点击数",
click_uv                  INT            COMMENT "点击UV",
exp_click_ratio           decimal(10,4)  COMMENT "曝光点击率",
user_click_ratio          decimal(10,4)  COMMENT "用户点击率",
cart_num                  INT            COMMENT "商品加购数",
cart_uv                   INT            COMMENT "加购UV",
sku_cart_ratio            decimal(10,4)  COMMENT "商品加购率",
user_cart_ratio           decimal(10,4)  COMMENT "用户加购率",
order_sku_num             INT            COMMENT "下单商品数",
order_uv                  INT            COMMENT "下单UV",
sku_order_ratio           decimal(10,4)  COMMENT "商品下单率",
user_order_ratio          decimal(10,4)  COMMENT "用户下单率",
purchase_num              INT            COMMENT "销量",  
pay_uv                    INT            COMMENT "付款uv",
pay_amount                INT            COMMENT "销售额",
sku_purchase_ratio        decimal(10,4)  COMMENT "商品购买转化率",
user_purchase_ratio       decimal(10,4)  COMMENT "用户购买转化率",
collect_uv                INT            COMMENT "商品收藏UV",
collect_num               INT            COMMENT "商品收藏数",
platform                  STRING         COMMENT "平台",
recommend_position        STRING         COMMENT "推荐位编号",
position_name             STRING         COMMENT "推荐位名称"
--glb_dc                    STRING         COMMENT "语言站",
--country                   STRING         COMMENT "国家"
)
COMMENT 'zaful APP推荐位数据报表新需求重写'
PARTITIONED BY (add_time STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\u0001' LINES TERMINATED BY '\n' STORED AS TEXTFILE
;




--页面PV 
CREATE TABLE IF NOT EXISTS dw_zaful_recommend.rewrite_app_report_pv_tmp(
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
CREATE TABLE IF NOT EXISTS dw_zaful_recommend.rewrite_app_report_uv_tmp(
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
CREATE TABLE IF NOT EXISTS dw_zaful_recommend.rewrite_app_report_exp_num_tmp(
exp_num                   INT            COMMENT "商品曝光数",
platform                  STRING         COMMENT "平台",
af_inner_mediasource      STRING         COMMENT "推荐位",
--country                   STRING         COMMENT "国家",
--glb_dc                    STRING         COMMENT "语言站",
add_time                  STRING         COMMENT "日期"
)
COMMENT "推荐位报表商品曝光数中间表"
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\u0001' LINES TERMINATED BY '\n' STORED AS TEXTFILE;


--查看商品UV	
CREATE TABLE IF NOT EXISTS dw_zaful_recommend.rewrite_app_report_sku_uv_tmp(
sku_uv                    INT            COMMENT "查看商品UV",
platform                  STRING         COMMENT "平台",
af_inner_mediasource      STRING         COMMENT "推荐位",
--country                   STRING         COMMENT "国家",
--glb_dc                    STRING         COMMENT "语言站",
add_time                  STRING         COMMENT "日期"
)
COMMENT "推荐位报表查看商品UV中间表"
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\u0001' LINES TERMINATED BY '\n' STORED AS TEXTFILE;


--商品点击数	
CREATE TABLE IF NOT EXISTS dw_zaful_recommend.rewrite_app_report_click_num_tmp(
click_num                 INT            COMMENT "商品点击数",
platform                  STRING         COMMENT "平台",
af_inner_mediasource      STRING         COMMENT "推荐位",
--country                   STRING         COMMENT "国家",
--glb_dc                    STRING         COMMENT "语言站",
add_time                  STRING         COMMENT "日期"
)
COMMENT "推荐位报表商品点击数中间表"
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\u0001' LINES TERMINATED BY '\n' STORED AS TEXTFILE;


--点击UV	
CREATE TABLE IF NOT EXISTS dw_zaful_recommend.rewrite_app_report_click_uv_tmp(
click_uv                  INT            COMMENT "点击UV",
platform                  STRING         COMMENT "平台",
af_inner_mediasource      STRING         COMMENT "推荐位",
--country                   STRING         COMMENT "国家",
--glb_dc                    STRING         COMMENT "语言站",
add_time                  STRING         COMMENT "日期"
)
COMMENT "推荐位报表商品点击UV中间表"
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\u0001' LINES TERMINATED BY '\n' STORED AS TEXTFILE;


--商品加购数	
CREATE TABLE IF NOT EXISTS dw_zaful_recommend.rewrite_app_report_cart_num_tmp(
cart_num                  INT            COMMENT "商品加购数",
platform                  STRING         COMMENT "平台",
af_inner_mediasource      STRING         COMMENT "推荐位",
--country                   STRING         COMMENT "国家",
--glb_dc                    STRING         COMMENT "语言站",
add_time                  STRING         COMMENT "日期"
)
COMMENT "推荐位报表商品加购数中间表"
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\u0001' LINES TERMINATED BY '\n' STORED AS TEXTFILE;


--加购UV	
CREATE TABLE IF NOT EXISTS dw_zaful_recommend.rewrite_app_report_cart_uv_tmp(
cart_uv                   INT            COMMENT "加购UV",
platform                  STRING         COMMENT "平台",
af_inner_mediasource      STRING         COMMENT "推荐位",
--country                   STRING         COMMENT "国家",
--glb_dc                    STRING         COMMENT "语言站",
add_time                  STRING         COMMENT "日期"
)
COMMENT "推荐位报表商品加购UV中间表"
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\u0001' LINES TERMINATED BY '\n' STORED AS TEXTFILE;


--下单商品数	

--中间表rewrite_report_sku_user_tmp：取sku,user_id
CREATE TABLE IF NOT EXISTS dw_zaful_recommend.rewrite_app_report_sku_user_country_tmp(
goods_sn                  STRING         COMMENT "sku",
user_id                   STRING         COMMENT "用户ID",
platform                  STRING         COMMENT "平台",
--country                   STRING         COMMENT "国家",
--glb_dc                    STRING         COMMENT "语言站",
af_inner_mediasource      STRING         COMMENT "推荐位编号"
)
COMMENT "推荐位报表user-sku-county中间表"
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\u0001' LINES TERMINATED BY '\n' STORED AS TEXTFILE;


--中间表rewrite-uv_report_sku_user_tmp：取sku,user_id
CREATE TABLE IF NOT EXISTS dw_zaful_recommend.rewrite_uv_app_report_sku_user_country_tmp(
goods_sn                  STRING         COMMENT "sku",
user_id                   STRING         COMMENT "用户ID",
appsflyer_device_id       STRING         COMMENT "用户uv",
platform                  STRING         COMMENT "平台",
--country                   STRING         COMMENT "国家",
--glb_dc                    STRING         COMMENT "语言站",
af_inner_mediasource      STRING         COMMENT "推荐位编号"
)
COMMENT "推荐位报表user-sku-county中间表"
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\u0001' LINES TERMINATED BY '\n' STORED AS TEXTFILE;



CREATE TABLE IF NOT EXISTS dw_zaful_recommend.rewrite_app_report_sku_user_tmp(
goods_sn                  STRING         COMMENT "sku",
user_id                   STRING         COMMENT "用户ID",
order_status              STRING         COMMENT "订单状态",
pay_amount                decimal(10,2)  COMMENT "购买金额",
goods_number              STRING         COMMENT "商品数量"
)
COMMENT "推荐位报表user-sku中间表"
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\u0001' LINES TERMINATED BY '\n' STORED AS TEXTFILE;


--下单商品数结果表
CREATE TABLE IF NOT EXISTS dw_zaful_recommend.rewrite_app_report_order_sku_num_tmp(
order_sku_num                 INT        COMMENT "下单商品数",
platform                  STRING         COMMENT "平台",
af_inner_mediasource      STRING         COMMENT "推荐位",
--country                   STRING         COMMENT "国家",
--glb_dc                    STRING         COMMENT "语言站",
add_time                  STRING         COMMENT "日期"
)
COMMENT "推荐位报表下单商品数中间表"
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\u0001' LINES TERMINATED BY '\n' STORED AS TEXTFILE;


--下单uv
CREATE TABLE IF NOT EXISTS dw_zaful_recommend.rewrite_app_report_order_uv_tmp(
order_uv                  INT            COMMENT "下单UV",
platform                  STRING         COMMENT "平台",
af_inner_mediasource      STRING         COMMENT "推荐位",
--country                   STRING         COMMENT "国家",
--glb_dc                    STRING         COMMENT "语言站",
add_time                  STRING         COMMENT "日期"
)
COMMENT "推荐位报表下单uv中间表"
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\u0001' LINES TERMINATED BY '\n' STORED AS TEXTFILE;


--销量
CREATE TABLE IF NOT EXISTS dw_zaful_recommend.rewrite_app_report_purchase_num_tmp(
purchase_num              INT            COMMENT "销量",
platform                  STRING         COMMENT "平台",
af_inner_mediasource      STRING         COMMENT "推荐位",
--country                   STRING         COMMENT "国家",
--glb_dc                    STRING         COMMENT "语言站",
add_time                  STRING         COMMENT "日期"
)
COMMENT "推荐位报表销量中间表"
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\u0001' LINES TERMINATED BY '\n' STORED AS TEXTFILE;



--付款uv
CREATE TABLE IF NOT EXISTS dw_zaful_recommend.rewrite_app_report_pay_uv_tmp(
pay_uv                    INT            COMMENT "付款uv",
platform                  STRING         COMMENT "平台",
af_inner_mediasource      STRING         COMMENT "推荐位",
--country                   STRING         COMMENT "国家",
--glb_dc                    STRING         COMMENT "语言站",
add_time                  STRING         COMMENT "日期"
)
COMMENT "推荐位报表付款uv中间表"
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\u0001' LINES TERMINATED BY '\n' STORED AS TEXTFILE;


--销售额
CREATE TABLE IF NOT EXISTS dw_zaful_recommend.rewrite_app_report_pay_amount_tmp(
pay_amount                decimal(10,2)  COMMENT "购买金额",
platform                  STRING         COMMENT "平台",
af_inner_mediasource      STRING         COMMENT "推荐位",
--country                   STRING         COMMENT "国家",
--glb_dc                    STRING         COMMENT "语言站",
add_time                  STRING         COMMENT "日期"
)
COMMENT "推荐位报表购买金额中间表"
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\u0001' LINES TERMINATED BY '\n' STORED AS TEXTFILE;


--商品收藏UV	
CREATE TABLE IF NOT EXISTS dw_zaful_recommend.rewrite_app_report_collect_uv_tmp(
collect_uv                INT            COMMENT "商品收藏UV",
platform                  STRING         COMMENT "平台",
af_inner_mediasource      STRING         COMMENT "推荐位",
--country                   STRING         COMMENT "国家",
--glb_dc                    STRING         COMMENT "语言站",
add_time                  STRING         COMMENT "日期"
)
COMMENT "推荐位报表商品收藏UV中间表"
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\u0001' LINES TERMINATED BY '\n' STORED AS TEXTFILE;

--商品收藏数	
CREATE TABLE IF NOT EXISTS dw_zaful_recommend.rewrite_app_report_collect_num_tmp(
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
INSERT OVERWRITE TABLE dw_zaful_recommend.rewrite_app_report_sku_user_country_tmp
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
      'all_featured_first'  AS af_inner_mediasource
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
  m.af_inner_mediasource
;


--中间表report_uv_sku_user_tmp：取sku,user_id
INSERT OVERWRITE TABLE dw_zaful_recommend.rewrite_uv_app_report_sku_user_country_tmp
SELECT
  m.goods_sn,
  n.customer_user_id,
  m.appsflyer_device_id,
  m.platform,
  m.af_inner_mediasource
FROM
  (
    SELECT
      appsflyer_device_id,
      --regexp_extract(event_value, '(.*?sku":")([0-9a-zA-Z]*)(".*?)', 2) AS sku,
      get_json_object(event_value, '$.af_content_id') AS goods_sn,
      platform,
      'all_featured_first'  AS af_inner_mediasource
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
  m.appsflyer_device_id,
  m.goods_sn,
  n.customer_user_id,
  m.platform,
  m.af_inner_mediasource
;


--中间表report_sku_user_tmp：取sku,user_id
INSERT OVERWRITE TABLE dw_zaful_recommend.rewrite_app_report_sku_user_tmp
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





--商品曝光数
INSERT OVERWRITE TABLE dw_zaful_recommend.rewrite_app_report_exp_num_tmp
SELECT
sum(m.exp_num) as exp_num,
m.platform,
m.af_inner_mediasource,
'${ADD_TIME}' as add_time
FROM
(
  SELECT
  platform,
  'all_featured_first' as af_inner_mediasource,
  length(get_json_object(event_value, '$.af_content_id'))-length(regexp_replace(get_json_object(event_value, '$.af_content_id'),',',''))+1 as exp_num
  FROM
  ods.ods_app_burial_log 
  WHERE
  concat_ws('-', year, month, day) = '${ADD_TIME}'
  and event_name='af_impression'
      and site='zaful'
) m 
group by
m.platform,
m.af_inner_mediasource
;


--查看商品UV
INSERT OVERWRITE TABLE dw_zaful_recommend.rewrite_app_report_sku_uv_tmp
SELECT
  count(*) AS sku_uv,
  m.platform,
  m.af_inner_mediasource,
  '${ADD_TIME}' as add_time
FROM
  (
    SELECT
      appsflyer_device_id,
      platform,
      'all_featured_first' as af_inner_mediasource
    FROM
      ods.ods_app_burial_log 
    WHERE
      concat_ws('-', year, month, day) = '${ADD_TIME}'
        and event_name='af_impression'
        and site='zaful'
    GROUP BY
      appsflyer_device_id,
      platform
  ) m
group by
  m.platform,
  m.af_inner_mediasource
  ;


--商品点击数
INSERT OVERWRITE TABLE dw_zaful_recommend.rewrite_app_report_click_num_tmp
SELECT
  count(*) AS click_num,
  platform,
  'all_featured_first' as af_inner_mediasource,
  '${ADD_TIME}' as add_time
FROM
  ods.ods_app_burial_log 
WHERE
  concat_ws('-', year, month, day) = '${ADD_TIME}'
  AND event_name='af_view_product'
  AND get_json_object(event_value, '$.af_changed_size_or_color') = 0
      and site='zaful'
group by
  platform
;


--点击UV
INSERT OVERWRITE TABLE dw_zaful_recommend.rewrite_app_report_click_uv_tmp
SELECT
  count(*) AS click_uv,
  m.platform,
  m.af_inner_mediasource,
  '${ADD_TIME}' as add_time
FROM
  (
    SELECT
      appsflyer_device_id,
      platform,
      'all_featured_first' as af_inner_mediasource
    FROM
      ods.ods_app_burial_log 
    WHERE
      concat_ws('-', year, month, day) = '${ADD_TIME}'
        and event_name='af_view_product'
		AND get_json_object(event_value, '$.af_changed_size_or_color') = 0
        and site='zaful'
    GROUP BY
      appsflyer_device_id,
      platform
  ) m
group by
  m.platform,
  m.af_inner_mediasource
  ;

  
  
--商品加购数
INSERT OVERWRITE TABLE dw_zaful_recommend.rewrite_app_report_cart_num_tmp
SELECT
sum(m.cart_num) as cart_num,
m.platform,
m.af_inner_mediasource,
'${ADD_TIME}' as add_time
FROM
(
  SELECT
  platform,
  'all_featured_first' as af_inner_mediasource,
  get_json_object(event_value, '$.af_quantity') as cart_num
  FROM
  ods.ods_app_burial_log 
  WHERE
  concat_ws('-', year, month, day) = '${ADD_TIME}'
  and event_name='af_add_to_bag'
      and site='zaful'
) m 
group by
m.platform,
m.af_inner_mediasource
;

--加购UV
INSERT OVERWRITE TABLE dw_zaful_recommend.rewrite_app_report_cart_uv_tmp
SELECT
  count(*) AS cart_uv,
  m.platform,
  m.af_inner_mediasource,
  '${ADD_TIME}' as add_time
FROM
  (
    SELECT
      appsflyer_device_id,
      platform,
      'all_featured_first' as af_inner_mediasource
    FROM
      ods.ods_app_burial_log 
    WHERE
      concat_ws('-', year, month, day) = '${ADD_TIME}'
        and event_name='af_add_to_bag'
        and site='zaful'
    GROUP BY
      appsflyer_device_id,
      platform
  ) m
group by
  m.platform,
  m.af_inner_mediasource
  ;


--下单商品数
INSERT OVERWRITE TABLE dw_zaful_recommend.rewrite_app_report_order_sku_num_tmp
SELECT
  SUM(x2.goods_number) AS order_num,
  x1.platform,
  x1.af_inner_mediasource,
  '${ADD_TIME}' as add_time
from
  dw_zaful_recommend.rewrite_app_report_sku_user_country_tmp x1
  INNER JOIN dw_zaful_recommend.rewrite_app_report_sku_user_tmp x2 ON x1.user_id = x2.user_id
  AND x1.goods_sn = x2.goods_sn
group by
  x1.platform,
  x1.af_inner_mediasource
;


--下单UV
INSERT OVERWRITE TABLE dw_zaful_recommend.rewrite_app_report_order_uv_tmp
SELECT
  count(distinct x1.appsflyer_device_id) as order_uv,
  x1.platform,
  x1.af_inner_mediasource,
  '${ADD_TIME}' as add_time
from
  dw_zaful_recommend.rewrite_uv_app_report_sku_user_country_tmp x1
  INNER JOIN dw_zaful_recommend.rewrite_app_report_sku_user_tmp x2 ON x1.user_id = x2.user_id
  AND x1.goods_sn = x2.goods_sn
group by
  x1.platform,
  x1.af_inner_mediasource
;

--销量
INSERT OVERWRITE TABLE dw_zaful_recommend.rewrite_app_report_purchase_num_tmp
SELECT
  SUM(x2.goods_number) AS purchase_num,
  x1.platform,
  x1.af_inner_mediasource,
  '${ADD_TIME}' as add_time
from
  dw_zaful_recommend.rewrite_app_report_sku_user_country_tmp x1
  INNER JOIN dw_zaful_recommend.rewrite_app_report_sku_user_tmp x2 ON x1.user_id = x2.user_id
  AND x1.goods_sn = x2.goods_sn
where
  x2.order_status not in ('0', '11')
group by
  x1.platform,
  x1.af_inner_mediasource
;


--付款UV
INSERT OVERWRITE TABLE dw_zaful_recommend.rewrite_app_report_pay_uv_tmp
SELECT
  count(distinct x1.appsflyer_device_id) as pay_uv,
  x1.platform,
  x1.af_inner_mediasource,
  '${ADD_TIME}' as add_time
from
  dw_zaful_recommend.rewrite_uv_app_report_sku_user_country_tmp x1
  INNER JOIN dw_zaful_recommend.rewrite_app_report_sku_user_tmp x2 ON x1.user_id = x2.user_id
  AND x1.goods_sn = x2.goods_sn
where
  x2.order_status not in ('0', '11')
group by
  x1.platform,
  x1.af_inner_mediasource
;



--销售额
INSERT OVERWRITE TABLE dw_zaful_recommend.rewrite_app_report_pay_amount_tmp
SELECT
  sum(x2.pay_amount) as pay_amount,
  x1.platform,
  x1.af_inner_mediasource,
  '${ADD_TIME}' as add_time
from
  dw_zaful_recommend.rewrite_app_report_sku_user_country_tmp x1
  INNER JOIN dw_zaful_recommend.rewrite_app_report_sku_user_tmp x2 ON x1.user_id = x2.user_id
  AND x1.goods_sn = x2.goods_sn
where
  x2.order_status not in ('0', '11')
group by
  x1.platform,
  x1.af_inner_mediasource
  ;


--商品收藏UV	
INSERT OVERWRITE TABLE dw_zaful_recommend.rewrite_app_report_collect_uv_tmp
SELECT
  count(distinct appsflyer_device_id) as collect_uv,
  platform,
  'all_featured_first' AS af_inner_mediasource,
  '${ADD_TIME}' as add_time
   FROM
  ods.ods_app_burial_log 
  WHERE
  concat_ws('-', year, month, day) = '${ADD_TIME}'
  and event_name='af_add_to_wishlist'
      and site='zaful'
  group by
    platform
;

  
 
--商品收藏数	
INSERT OVERWRITE TABLE dw_zaful_recommend.rewrite_app_report_collect_num_tmp
SELECT
  count(*) as collect_num,
  platform,
  'all_featured_first' AS af_inner_mediasource,
  '${ADD_TIME}' as add_time
   FROM
  ods.ods_app_burial_log 
  WHERE
  concat_ws('-', year, month, day) = '${ADD_TIME}'
  and event_name='af_add_to_wishlist'
      and site='zaful'
group by
  platform
;




--所有结果汇总
INSERT into TABLE dw_zaful_recommend.rewrite_zaful_app_recommend_report_featured_first PARTITION (add_time = '${ADD_TIME}')
select
  c.exp_num,
  d.sku_uv,
  e.click_num,
  f.click_uv,
  e.click_num / c.exp_num,
  f.click_uv / d.sku_uv,
  g.cart_num,
  h.cart_uv,
  g.cart_num / c.exp_num,
  h.cart_uv / d.sku_uv,
  i.order_sku_num,
  j.order_uv,
  i.order_sku_num / g.cart_num,
  j.order_uv / d.sku_uv,
  k.purchase_num,
  l.pay_uv,
  m.pay_amount,
  k.purchase_num / c.exp_num,
  l.pay_uv / d.sku_uv,
  n.collect_uv,
  o.collect_num,
  'android',
  'all_featured_first',
  '全部推荐位'
from   
(select '${ADD_TIME}' as add_time) mm 
left join 
(select exp_num, add_time from dw_zaful_recommend.rewrite_app_report_exp_num_tmp where af_inner_mediasource='all_featured_first' and platform='android') c on mm.add_time=c.add_time
left join 
(select sku_uv, add_time from dw_zaful_recommend.rewrite_app_report_sku_uv_tmp where af_inner_mediasource='all_featured_first' and platform='android') d on mm.add_time=d.add_time
left join 
(select click_num, add_time from dw_zaful_recommend.rewrite_app_report_click_num_tmp where af_inner_mediasource='all_featured_first' and platform='android') e on mm.add_time=e.add_time
left join 
(select click_uv, add_time from dw_zaful_recommend.rewrite_app_report_click_uv_tmp where af_inner_mediasource='all_featured_first' and platform='android') f on mm.add_time=f.add_time
left join 
(select cart_num, add_time from dw_zaful_recommend.rewrite_app_report_cart_num_tmp where af_inner_mediasource='all_featured_first' and platform='android') g on mm.add_time=g.add_time
left join 
(select cart_uv, add_time from dw_zaful_recommend.rewrite_app_report_cart_uv_tmp where af_inner_mediasource='all_featured_first' and platform='android') h on mm.add_time=h.add_time
left join 
(select order_sku_num, add_time from dw_zaful_recommend.rewrite_app_report_order_sku_num_tmp where af_inner_mediasource='all_featured_first' and platform='android') i on mm.add_time=i.add_time
left join 
(select order_uv, add_time from dw_zaful_recommend.rewrite_app_report_order_uv_tmp where af_inner_mediasource='all_featured_first' and platform='android') j on mm.add_time=j.add_time
left join 
(select purchase_num, add_time from dw_zaful_recommend.rewrite_app_report_purchase_num_tmp where af_inner_mediasource='all_featured_first' and platform='android') k on mm.add_time=k.add_time
left join 
(select pay_uv, add_time from dw_zaful_recommend.rewrite_app_report_pay_uv_tmp where af_inner_mediasource='all_featured_first' and platform='android') l on mm.add_time=l.add_time
left join 
(select pay_amount, add_time from dw_zaful_recommend.rewrite_app_report_pay_amount_tmp where af_inner_mediasource='all_featured_first' and platform='android') m on mm.add_time=m.add_time
left join 
(select collect_uv, add_time from dw_zaful_recommend.rewrite_app_report_collect_uv_tmp where af_inner_mediasource='all_featured_first' and platform='android') n on mm.add_time=n.add_time
left join 
(select collect_num, add_time from dw_zaful_recommend.rewrite_app_report_collect_num_tmp where af_inner_mediasource='all_featured_first' and platform='android') o on mm.add_time=o.add_time
union all
select
  c.exp_num,
  d.sku_uv,
  e.click_num,
  f.click_uv,
  e.click_num / c.exp_num,
  f.click_uv / d.sku_uv,
  g.cart_num,
  h.cart_uv,
  g.cart_num / c.exp_num,
  h.cart_uv / d.sku_uv,
  i.order_sku_num,
  j.order_uv,
  i.order_sku_num / g.cart_num,
  j.order_uv / d.sku_uv,
  k.purchase_num,
  l.pay_uv,
  m.pay_amount,
  k.purchase_num / c.exp_num,
  l.pay_uv / d.sku_uv,
  n.collect_uv,
  o.collect_num,
  'ios',
  'all_featured_first',
  '全部推荐位'
from   
(select '${ADD_TIME}' as add_time) mm 
left join 
(select exp_num, add_time from dw_zaful_recommend.rewrite_app_report_exp_num_tmp where af_inner_mediasource='all_featured_first' and platform='ios') c on mm.add_time=c.add_time
left join 
(select sku_uv, add_time from dw_zaful_recommend.rewrite_app_report_sku_uv_tmp where af_inner_mediasource='all_featured_first' and platform='ios') d on mm.add_time=d.add_time
left join 
(select click_num, add_time from dw_zaful_recommend.rewrite_app_report_click_num_tmp where af_inner_mediasource='all_featured_first' and platform='ios') e on mm.add_time=e.add_time
left join 
(select click_uv, add_time from dw_zaful_recommend.rewrite_app_report_click_uv_tmp where af_inner_mediasource='all_featured_first' and platform='ios') f on mm.add_time=f.add_time
left join 
(select cart_num, add_time from dw_zaful_recommend.rewrite_app_report_cart_num_tmp where af_inner_mediasource='all_featured_first' and platform='ios') g on mm.add_time=g.add_time
left join 
(select cart_uv, add_time from dw_zaful_recommend.rewrite_app_report_cart_uv_tmp where af_inner_mediasource='all_featured_first' and platform='ios') h on mm.add_time=h.add_time
left join 
(select order_sku_num, add_time from dw_zaful_recommend.rewrite_app_report_order_sku_num_tmp where af_inner_mediasource='all_featured_first' and platform='ios') i on mm.add_time=i.add_time
left join 
(select order_uv, add_time from dw_zaful_recommend.rewrite_app_report_order_uv_tmp where af_inner_mediasource='all_featured_first' and platform='ios') j on mm.add_time=j.add_time
left join 
(select purchase_num, add_time from dw_zaful_recommend.rewrite_app_report_purchase_num_tmp where af_inner_mediasource='all_featured_first' and platform='ios') k on mm.add_time=k.add_time
left join 
(select pay_uv, add_time from dw_zaful_recommend.rewrite_app_report_pay_uv_tmp where af_inner_mediasource='all_featured_first' and platform='ios') l on mm.add_time=l.add_time
left join 
(select pay_amount, add_time from dw_zaful_recommend.rewrite_app_report_pay_amount_tmp where af_inner_mediasource='all_featured_first' and platform='ios') m on mm.add_time=m.add_time
left join 
(select collect_uv, add_time from dw_zaful_recommend.rewrite_app_report_collect_uv_tmp where af_inner_mediasource='all_featured_first' and platform='ios') n on mm.add_time=n.add_time
left join 
(select collect_num, add_time from dw_zaful_recommend.rewrite_app_report_collect_num_tmp where af_inner_mediasource='all_featured_first' and platform='ios') o on mm.add_time=o.add_time
;