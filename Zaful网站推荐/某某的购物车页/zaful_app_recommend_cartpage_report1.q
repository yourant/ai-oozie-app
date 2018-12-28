
--@author wuchao
--@date 2018年12月11日 
--@desc  Zaful App购物车页


SET mapred.job.name=zaful_app_recommend_cartpage_wuc_report;
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
CREATE TABLE IF NOT EXISTS dw_zaful_recommend.zaful_app_recommend_cartpage_wuc_report(
pv                        INT            COMMENT "页面PV",
uv                        INT            COMMENT "页面UV",
position_exp_num          INT            COMMENT "坑位曝光",
position_uv               INT            COMMENT "坑位uv",
exp_num                   INT            COMMENT "商品曝光数",
click_num                 INT            COMMENT "商品点击数",
click_uv                  INT            COMMENT "点击UV",
sku_click_ratio           decimal(10,4)  COMMENT "商品点击率",
cart_num                  INT            COMMENT "商品加购数",
cart_uv                   INT            COMMENT "加购UV",
sku_cart_ratio            decimal(10,4)  COMMENT "商品加购率",
user_cart_ratio           decimal(10,4)  COMMENT "加购率",
order_sku_num             INT            COMMENT "下单商品数",
order_sku_radio           decimal(10,4)  COMMENT "下单商品转化率",
paid_sku                  INT            COMMENT "付款商品数",
paid_amount               INT            COMMENT "付款金额",
sku_add_count             INT            COMMENT "商品加收次数",  
user_add_count            INT            COMMENT "加收用户数",
gmv                       INT            COMMENT "gmv",
gmv_cost_mille            decimal(10,4)  COMMENT "千次曝光GMV",
order_user_count          INT            COMMENT "下单客户数",
pay_uv                    INT            COMMENT "付款客户数",
paid_user                 decimal(10,4)  COMMENT "客单价",
platform                  STRING         COMMENT "平台",
recommend_position        STRING         COMMENT "列表页编号",
position_name             STRING         COMMENT "列表页名称",
language                    STRING         COMMENT "语言站",
country_code                   STRING         COMMENT "国家"
)
COMMENT 'zaful APP购物车数据报表新需求'
PARTITIONED BY (add_time STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\u0001' LINES TERMINATED BY '\n' STORED AS TEXTFILE
;

--输出结果表导出表
CREATE TABLE IF NOT EXISTS dw_zaful_recommend.zaful_app_recommend_cartpage_wuc_report_exp(
pv                        INT            COMMENT "页面PV",
uv                        INT            COMMENT "页面UV",
position_exp_num          INT            COMMENT "坑位曝光",
position_uv               INT            COMMENT "坑位uv",
exp_num                   INT            COMMENT "商品曝光数",
click_num                 INT            COMMENT "商品点击数",
click_uv                  INT            COMMENT "点击UV",
sku_click_ratio           decimal(10,4)  COMMENT "商品点击率",
cart_num                  INT            COMMENT "商品加购数",
cart_uv                   INT            COMMENT "加购UV",
sku_cart_ratio            decimal(10,4)  COMMENT "商品加购率",
user_cart_ratio           decimal(10,4)  COMMENT "加购率",
order_sku_num             INT            COMMENT "下单商品数",
order_sku_radio           decimal(10,4)  COMMENT "下单商品转化率",
paid_sku                  INT            COMMENT "付款商品数",
paid_amount               INT            COMMENT "付款金额",
sku_add_count             INT            COMMENT "商品加收次数",  
user_add_count            INT            COMMENT "加收用户数",
gmv                       INT            COMMENT "gmv",
gmv_cost_mille            decimal(10,4)  COMMENT "千次曝光GMV",
order_user_count          INT            COMMENT "下单客户数",
pay_uv                    INT            COMMENT "付款客户数",
paid_user                 decimal(10,4)  COMMENT "客单价",
platform                  STRING         COMMENT "平台",
recommend_position        STRING         COMMENT "列表页编号",
position_name             STRING         COMMENT "列表页名称",
language                    STRING         COMMENT "语言站",
country_code                   STRING         COMMENT "国家",
add_time                  STRING         comment ""
)
;


--页面PV 
CREATE TABLE IF NOT EXISTS dw_zaful_recommend.zaful_app_recommend_cartpage_wuc_pv_tmp(
pv                        INT            COMMENT "页面PV",
platform                  STRING         COMMENT "平台",
af_inner_mediasource      STRING         COMMENT "列表页",
language                    STRING         COMMENT "语言站",
country_code                   STRING         COMMENT "国家",
add_time                  STRING         COMMENT "日期"
)
COMMENT "列表页报表页面PV中间表"
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\u0001' LINES TERMINATED BY '\n' STORED AS TEXTFILE;



--页面UV	
CREATE TABLE IF NOT EXISTS dw_zaful_recommend.zaful_app_recommend_cartpage_wuc_uv_tmp(
uv                        INT            COMMENT "页面UV",
platform                  STRING         COMMENT "平台",
af_inner_mediasource      STRING         COMMENT "列表页",
language                    STRING         COMMENT "语言站",
country_code                   STRING         COMMENT "国家",
add_time                  STRING         COMMENT "日期"
)
COMMENT "列表页报表页面UV中间表"
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\u0001' LINES TERMINATED BY '\n' STORED AS TEXTFILE;

--坑位曝光数	
CREATE TABLE IF NOT EXISTS dw_zaful_recommend.zaful_app_recommend_cartpage_wuc_position_exp_num_tmp(
position_exp_num                   INT            COMMENT "坑位曝光数",
platform                  STRING         COMMENT "平台",
af_inner_mediasource      STRING         COMMENT "列表页",
language                    STRING         COMMENT "语言站",
country_code                   STRING         COMMENT "国家",
add_time                  STRING         COMMENT "日期"
)
COMMENT "列表页报表商品曝光数中间表"
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\u0001' LINES TERMINATED BY '\n' STORED AS TEXTFILE;

--坑位uv	
CREATE TABLE IF NOT EXISTS dw_zaful_recommend.zaful_app_recommend_cartpage_wuc_position_uv_tmp(
position_uv                   INT            COMMENT "坑位曝光数",
platform                  STRING         COMMENT "平台",
af_inner_mediasource      STRING         COMMENT "列表页",
language                    STRING         COMMENT "语言站",
country_code                   STRING         COMMENT "国家",
add_time                  STRING         COMMENT "日期"
)
COMMENT "列表页报表商品曝光数中间表"
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\u0001' LINES TERMINATED BY '\n' STORED AS TEXTFILE;

--商品曝光数	
CREATE TABLE IF NOT EXISTS dw_zaful_recommend.zaful_app_recommend_cartpage_wuc_exp_num_tmp(
exp_num                   INT            COMMENT "商品曝光数",
platform                  STRING         COMMENT "平台",
af_inner_mediasource      STRING         COMMENT "列表页",
language                    STRING         COMMENT "语言站",
country_code                   STRING         COMMENT "国家",
add_time                  STRING         COMMENT "日期"
)
COMMENT "列表页报表商品曝光数中间表"
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\u0001' LINES TERMINATED BY '\n' STORED AS TEXTFILE;



--商品点击数	
CREATE TABLE IF NOT EXISTS dw_zaful_recommend.zaful_app_recommend_cartpage_wuc_click_num_tmp(
click_num                 INT            COMMENT "商品点击数",
platform                  STRING         COMMENT "平台",
af_inner_mediasource      STRING         COMMENT "列表页",
language                    STRING         COMMENT "语言站",
country_code                   STRING         COMMENT "国家",
add_time                  STRING         COMMENT "日期"
)
COMMENT "列表页报表商品点击数中间表"
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\u0001' LINES TERMINATED BY '\n' STORED AS TEXTFILE;


--点击UV	
CREATE TABLE IF NOT EXISTS dw_zaful_recommend.zaful_app_recommend_cartpage_wuc_click_uv_tmp(
click_uv                  INT            COMMENT "点击UV",
platform                  STRING         COMMENT "平台",
af_inner_mediasource      STRING         COMMENT "列表页",
language                    STRING         COMMENT "语言站",
country_code                   STRING         COMMENT "国家",
add_time                  STRING         COMMENT "日期"
)
COMMENT "列表页报表商品点击UV中间表"
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\u0001' LINES TERMINATED BY '\n' STORED AS TEXTFILE;


--商品加购数	
CREATE TABLE IF NOT EXISTS dw_zaful_recommend.zaful_app_recommend_cartpage_wuc_cart_num_tmp(
cart_num                  INT            COMMENT "商品加购数",
platform                  STRING         COMMENT "平台",
af_inner_mediasource      STRING         COMMENT "列表页",
language                    STRING         COMMENT "语言站",
country_code                   STRING         COMMENT "国家",
add_time                  STRING         COMMENT "日期"
)
COMMENT "列表页报表商品加购数中间表"
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\u0001' LINES TERMINATED BY '\n' STORED AS TEXTFILE;


--加购UV	
CREATE TABLE IF NOT EXISTS dw_zaful_recommend.zaful_app_recommend_cartpage_wuc_cart_uv_tmp(
cart_uv                   INT            COMMENT "加购UV",
platform                  STRING         COMMENT "平台",
af_inner_mediasource      STRING         COMMENT "列表页",
language                    STRING         COMMENT "语言站",
country_code                   STRING         COMMENT "国家",
add_time                  STRING         COMMENT "日期"
)
COMMENT "列表页报表商品加购UV中间表"
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\u0001' LINES TERMINATED BY '\n' STORED AS TEXTFILE;


--下单商品数	

--中间表rewrite_report_sku_user_tmp：取sku,user_id
CREATE TABLE IF NOT EXISTS dw_zaful_recommend.zaful_app_recommend_cartpage_wuc_sku_user_country_tmp(
goods_sn                  STRING         COMMENT "sku",
user_id                   STRING         COMMENT "用户ID",
platform                  STRING         COMMENT "平台",
af_inner_mediasource      STRING         COMMENT "列表页编号",
language                    STRING         COMMENT "语言站",
country_code                   STRING         COMMENT "国家"
)
COMMENT "列表页报表user-sku-county中间表"
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\u0001' LINES TERMINATED BY '\n' STORED AS TEXTFILE;


--中间表rewrite-uv_report_sku_user_tmp：取sku,user_id
CREATE TABLE IF NOT EXISTS dw_zaful_recommend.zaful_app_recommend_cartpage_wuc_device_id_sku_user_country_tmp(
goods_sn                  STRING         COMMENT "sku",
user_id                   STRING         COMMENT "用户ID",
appsflyer_device_id       STRING         COMMENT "用户uv",
platform                  STRING         COMMENT "平台",
af_inner_mediasource      STRING         COMMENT "列表页编号",
language                    STRING         COMMENT "语言站",
country_code                   STRING         COMMENT "国家"
)
COMMENT "列表页报表user-sku-county中间表"
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\u0001' LINES TERMINATED BY '\n' STORED AS TEXTFILE;



CREATE TABLE IF NOT EXISTS dw_zaful_recommend.zaful_app_recommend_cartpage_wuc_order_info_good_tmp(
goods_sn                  STRING         COMMENT "sku",
user_id                   STRING         COMMENT "用户ID",
order_status              STRING         COMMENT "订单状态",
pay_amount                decimal(10,2)  COMMENT "购买金额",
goods_number              STRING         COMMENT "商品数量"
)
COMMENT "列表页报表user-sku中间表"
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\u0001' LINES TERMINATED BY '\n' STORED AS TEXTFILE;


--下单商品数
CREATE TABLE IF NOT EXISTS dw_zaful_recommend.zaful_app_recommend_cartpage_wuc_order_sku_num_tmp(
order_sku_num                 INT        COMMENT "下单商品数",
platform                  STRING         COMMENT "平台",
af_inner_mediasource      STRING         COMMENT "列表页",
language                    STRING         COMMENT "语言站",
country_code                   STRING         COMMENT "国家",
add_time                  STRING         COMMENT "日期"
)
COMMENT "列表页报表下单商品数中间表"
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\u0001' LINES TERMINATED BY '\n' STORED AS TEXTFILE;


--付款商品数
CREATE TABLE IF NOT EXISTS dw_zaful_recommend.zaful_app_recommend_cartpage_wuc_paid_sku_tmp(
paid_sku                  INT            COMMENT "付款商品数",
platform                  STRING         COMMENT "平台",
af_inner_mediasource      STRING         COMMENT "列表页",
language                    STRING         COMMENT "语言站",
country_code                   STRING         COMMENT "国家",
add_time                  STRING         COMMENT "日期"
)
COMMENT "付款商品数"
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\u0001' LINES TERMINATED BY '\n' STORED AS TEXTFILE;

--付款金额
CREATE TABLE IF NOT EXISTS dw_zaful_recommend.zaful_app_recommend_cartpage_wuc_paid_amount_tmp(
paid_amount                  INT            COMMENT "付款金额",
platform                  STRING         COMMENT "平台",
af_inner_mediasource      STRING         COMMENT "列表页",
language                    STRING         COMMENT "语言站",
country_code                   STRING         COMMENT "国家",
add_time                  STRING         COMMENT "日期"
)
COMMENT "付款金额"
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\u0001' LINES TERMINATED BY '\n' STORED AS TEXTFILE;

--商品加收次数
CREATE TABLE IF NOT EXISTS dw_zaful_recommend.zaful_app_recommend_cartpage_wuc_sku_add_count_tmp(
sku_add_count                  INT            COMMENT "商品加收次数",
platform                  STRING         COMMENT "平台",
af_inner_mediasource      STRING         COMMENT "列表页",
language                    STRING         COMMENT "语言站",
country_code                   STRING         COMMENT "国家",
add_time                  STRING         COMMENT "日期"
)
COMMENT "商品加收次数"
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\u0001' LINES TERMINATED BY '\n' STORED AS TEXTFILE;

--加收用户数
CREATE TABLE IF NOT EXISTS dw_zaful_recommend.zaful_app_recommend_cartpage_wuc_user_add_count_tmp(
user_add_count                  INT            COMMENT "加收用户数",
platform                  STRING         COMMENT "平台",
af_inner_mediasource      STRING         COMMENT "列表页",
language                    STRING         COMMENT "语言站",
country_code                   STRING         COMMENT "国家",
add_time                  STRING         COMMENT "日期"
)
COMMENT "加收用户数"
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\u0001' LINES TERMINATED BY '\n' STORED AS TEXTFILE;

--GMV
CREATE TABLE IF NOT EXISTS dw_zaful_recommend.zaful_app_recommend_cartpage_wuc_gmv_tmp(
gmv              INT            COMMENT "gmv",
platform                  STRING         COMMENT "平台",
af_inner_mediasource      STRING         COMMENT "列表页",
language                    STRING         COMMENT "语言站",
country_code                   STRING         COMMENT "国家",
add_time                  STRING         COMMENT "日期"
)
COMMENT "列表页报表销量中间表"
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\u0001' LINES TERMINATED BY '\n' STORED AS TEXTFILE;

--下单客户数
CREATE TABLE IF NOT EXISTS dw_zaful_recommend.zaful_app_recommend_cartpage_wuc_order_user_count_tmp(
order_user_count              INT            COMMENT "下单客户数",
platform                  STRING         COMMENT "平台",
af_inner_mediasource      STRING         COMMENT "列表页",
language                    STRING         COMMENT "语言站",
country_code                   STRING         COMMENT "国家",
add_time                  STRING         COMMENT "日期"
)
COMMENT "下单客户数"
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\u0001' LINES TERMINATED BY '\n' STORED AS TEXTFILE;



--付款客户数
CREATE TABLE IF NOT EXISTS dw_zaful_recommend.zaful_app_recommend_cartpage_wuc_pay_uv_tmp(
pay_uv                    INT            COMMENT "付款uv",
platform                  STRING         COMMENT "平台",
af_inner_mediasource      STRING         COMMENT "列表页",
language                    STRING         COMMENT "语言站",
country_code                   STRING         COMMENT "国家",
add_time                  STRING         COMMENT "日期"
)
COMMENT "列表页报表付款uv中间表"
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
INSERT OVERWRITE TABLE dw_zaful_recommend.zaful_app_recommend_cartpage_wuc_sku_user_country_tmp
SELECT
  m.goods_sn,
  n.customer_user_id,
  m.platform,
  m.af_inner_mediasource,
  m.language,
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
      concat_ws('-', year, month, day) BETWEEN '${ADD_TIME_W}'
      AND '${ADD_TIME}'
      AND event_name='af_add_to_bag'
      and site='zaful'
      AND get_json_object(event_value, '$.af_content_id') is not null 
      and get_json_object(event_value, '$.af_inner_mediasource') ='recommend_cartpage'
  ) m
  INNER JOIN dw_zaful_recommend.zaful_app_u_map n ON m.appsflyer_device_id = n.appsflyer_device_id
GROUP BY
  m.goods_sn,
  n.customer_user_id,
  m.platform,
  m.af_inner_mediasource,
  m.language,
  m.country_code
;


--中间表report_uv_sku_user_tmp：取sku,user_id
INSERT OVERWRITE TABLE dw_zaful_recommend.zaful_app_recommend_cartpage_wuc_device_id_sku_user_country_tmp
SELECT
  m.goods_sn,
  n.customer_user_id,
  m.appsflyer_device_id,
  m.platform,
  m.af_inner_mediasource,
  m.language,
  m.country_code
FROM
  (
    SELECT
      appsflyer_device_id,
      --regexp_extract(event_value, '(.*?sku":")([0-9a-zA-Z]*)(".*?)', 2) AS sku,
      get_json_object(event_value, '$.af_content_id') AS goods_sn,
      platform,
      get_json_object(event_value, '$.af_inner_mediasource')  AS af_inner_mediasource,
      language,
      country_code
    FROM
      ods.ods_app_burial_log 
    WHERE
      concat_ws('-', year, month, day) BETWEEN '${ADD_TIME_W}'
      AND '${ADD_TIME}'
      AND event_name='af_add_to_bag'
      and site='zaful'
      AND get_json_object(event_value, '$.af_content_id') is not null 
      and get_json_object(event_value, '$.af_inner_mediasource') ='recommend_cartpage'
  ) m
  INNER JOIN dw_zaful_recommend.zaful_app_u_map n ON m.appsflyer_device_id = n.appsflyer_device_id
GROUP BY
  m.appsflyer_device_id,
  m.goods_sn,
  n.customer_user_id,
  m.platform,
  m.af_inner_mediasource,
  m.language,
  m.country_code
;


--中间表report_sku_user_tmp：取sku,user_id
INSERT OVERWRITE TABLE dw_zaful_recommend.zaful_app_recommend_cartpage_wuc_order_info_good_tmp
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
INSERT OVERWRITE TABLE dw_zaful_recommend.zaful_app_recommend_cartpage_wuc_pv_tmp
SELECT
  count( m.appsflyer_device_id) AS pv,
  m.platform,
  m.af_inner_mediasource,
  m.language,
  m.country_code,
  '${ADD_TIME}' as add_time
FROM
  (
    SELECT
      appsflyer_device_id,
      platform,
      'recommend_cartpage' as af_inner_mediasource,
      language,
      country_code
    FROM
      ods.ods_app_burial_log 
    WHERE
      concat_ws('-', year, month, day) = '${ADD_TIME}'
        and event_name='af_view_cartpage'
        and site='zaful'
  ) m
group by
  m.platform,
  m.af_inner_mediasource,
  m.language,
  m.country_code
  ;


--页面UV
INSERT OVERWRITE TABLE dw_zaful_recommend.zaful_app_recommend_cartpage_wuc_uv_tmp
SELECT
  count(distinct m.appsflyer_device_id) AS uv,
  m.platform,
  m.af_inner_mediasource,
  m.language,
  m.country_code,
  '${ADD_TIME}' as add_time
FROM
  (
    SELECT
      appsflyer_device_id,
      platform,
      'recommend_cartpage' as af_inner_mediasource,
      language,
      country_code
    FROM
      ods.ods_app_burial_log 
    WHERE
      concat_ws('-', year, month, day) = '${ADD_TIME}'
        and event_name='af_view_cartpage'
        and site='zaful'
  ) m
group by
  m.platform,
  m.af_inner_mediasource,
  m.language,
  m.country_code
  ;

--坑位曝光数
INSERT OVERWRITE TABLE  dw_zaful_recommend.zaful_app_recommend_cartpage_wuc_position_exp_num_tmp
SELECT
count(m.appsflyer_device_id) as position_exp_num,
m.platform,
m.af_inner_mediasource,
m.language,
m.country_code,
'${ADD_TIME}' as add_time
FROM
(
  SELECT
  appsflyer_device_id,
  platform,
  get_json_object(event_value, '$.af_inner_mediasource') as af_inner_mediasource,
  language,
  country_code
  FROM
  ods.ods_app_burial_log 
  WHERE
  concat_ws('-', year, month, day) = '${ADD_TIME}'
  and event_name='af_impression'
  and get_json_object(event_value, '$.af_inner_mediasource') ='recommend_cartpage'
      and site='zaful'
) m 
group by
m.platform,
m.af_inner_mediasource,
m.language,
m.country_code
;



--坑位uv
INSERT OVERWRITE TABLE  dw_zaful_recommend.zaful_app_recommend_cartpage_wuc_position_uv_tmp
SELECT
count(distinct m.appsflyer_device_id) as position_uv,
m.platform,
m.af_inner_mediasource,
m.language,
m.country_code,
'${ADD_TIME}' as add_time
FROM
(
  SELECT
  appsflyer_device_id,
  platform,
  get_json_object(event_value, '$.af_inner_mediasource') as af_inner_mediasource,
  language,
  country_code
  FROM
  ods.ods_app_burial_log 
  WHERE
  concat_ws('-', year, month, day) = '${ADD_TIME}'
  and event_name='af_impression'
  and get_json_object(event_value, '$.af_inner_mediasource') ='recommend_cartpage'
      and site='zaful'
) m 
group by
m.platform,
m.af_inner_mediasource,
m.language,
m.country_code
;


--商品曝光数
INSERT OVERWRITE TABLE dw_zaful_recommend.zaful_app_recommend_cartpage_wuc_exp_num_tmp
SELECT
sum(m.exp_num) as exp_num,
m.platform,
m.af_inner_mediasource,
m.language,
m.country_code,
'${ADD_TIME}' as add_time
FROM
(
  SELECT
  platform,
  get_json_object(event_value, '$.af_inner_mediasource') as af_inner_mediasource,
  length(get_json_object(event_value, '$.af_content_id'))-length(regexp_replace(get_json_object(event_value, '$.af_content_id'),',',''))+1 as exp_num,
  language,
  country_code
  FROM
  ods.ods_app_burial_log 
  WHERE
  concat_ws('-', year, month, day) = '${ADD_TIME}'
  and event_name='af_impression'
  and get_json_object(event_value, '$.af_inner_mediasource') ='recommend_cartpage'
      and site='zaful'
) m 
group by
m.platform,
m.af_inner_mediasource,
m.language,
m.country_code
;





--商品点击数
INSERT OVERWRITE TABLE dw_zaful_recommend.zaful_app_recommend_cartpage_wuc_click_num_tmp
SELECT
  count(appsflyer_device_id) AS click_num,
  platform,
  get_json_object(event_value, '$.af_inner_mediasource') as af_inner_mediasource,
  language,
  country_code,
  '${ADD_TIME}' as add_time
FROM
  ods.ods_app_burial_log 
WHERE
  concat_ws('-', year, month, day) = '${ADD_TIME}'
  AND event_name='af_view_product'
  AND get_json_object(event_value, '$.af_changed_size_or_color') = 0
  and get_json_object(event_value, '$.af_inner_mediasource') ='recommend_cartpage'
      and site='zaful'
group by
  platform,
  get_json_object(event_value, '$.af_inner_mediasource'),
  language,
  country_code
;


--点击UV
INSERT OVERWRITE TABLE dw_zaful_recommend.zaful_app_recommend_cartpage_wuc_click_uv_tmp
SELECT
  count(distinct m.appsflyer_device_id) AS click_uv,
  m.platform,
  m.af_inner_mediasource,
  m.language,
  m.country_code,
  '${ADD_TIME}' as add_time
FROM
  (
    SELECT
      appsflyer_device_id,
      platform,
      get_json_object(event_value, '$.af_inner_mediasource') as af_inner_mediasource,
      language,
      country_code
    FROM
      ods.ods_app_burial_log 
    WHERE
      concat_ws('-', year, month, day) = '${ADD_TIME}'
        and event_name='af_view_product'
		AND get_json_object(event_value, '$.af_changed_size_or_color') = 0
		and get_json_object(event_value, '$.af_inner_mediasource') ='recommend_cartpage'
        and site='zaful'

  ) m
group by
  m.platform,
  m.af_inner_mediasource,
  m.language,
  m.country_code
  ;

  
  
--商品加购数
INSERT OVERWRITE TABLE dw_zaful_recommend.zaful_app_recommend_cartpage_wuc_cart_num_tmp
SELECT
sum(m.cart_num) as cart_num,
m.platform,
m.af_inner_mediasource,
m.language,
m.country_code,
'${ADD_TIME}' as add_time
FROM
(
  SELECT
  platform,
  get_json_object(event_value, '$.af_inner_mediasource') as af_inner_mediasource,
  get_json_object(event_value, '$.af_quantity') as cart_num,
  language,
  country_code
  FROM
  ods.ods_app_burial_log 
  WHERE
  concat_ws('-', year, month, day) = '${ADD_TIME}'
  and event_name='af_add_to_bag'
  and get_json_object(event_value, '$.af_inner_mediasource') ='recommend_cartpage'
      and site='zaful'
) m 
group by
m.platform,
m.af_inner_mediasource,
m.language,
m.country_code
;

--加购UV
INSERT OVERWRITE TABLE dw_zaful_recommend.zaful_app_recommend_cartpage_wuc_cart_uv_tmp
SELECT
  count(distinct m.appsflyer_device_id) AS cart_uv,
  m.platform,
  m.af_inner_mediasource,
  m.language,
  m.country_code,
  '${ADD_TIME}' as add_time
FROM
  (
    SELECT
      appsflyer_device_id,
      platform,
      get_json_object(event_value, '$.af_inner_mediasource') as af_inner_mediasource,
      language,
      country_code
    FROM
      ods.ods_app_burial_log 
    WHERE
      concat_ws('-', year, month, day) = '${ADD_TIME}'
        and event_name='af_add_to_bag'
		    and get_json_object(event_value, '$.af_inner_mediasource') ='recommend_cartpage'
        and site='zaful'
  ) m
group by
  m.platform,
  m.af_inner_mediasource,
  m.language,
  m.country_code
  ;


--下单商品数
INSERT OVERWRITE TABLE dw_zaful_recommend.zaful_app_recommend_cartpage_wuc_order_sku_num_tmp
SELECT
  SUM(x2.goods_number) AS order_sku_num,
  x1.platform,
  x1.af_inner_mediasource,
  x1.language,
  x1.country_code,
  '${ADD_TIME}' as add_time
from
  dw_zaful_recommend.zaful_app_recommend_cartpage_wuc_sku_user_country_tmp x1
  INNER JOIN dw_zaful_recommend.zaful_app_recommend_cartpage_wuc_order_info_good_tmp x2 ON x1.user_id = x2.user_id
  AND x1.goods_sn = x2.goods_sn
group by
  x1.platform,
  x1.af_inner_mediasource,
  x1.language,
  x1.country_code
;


--付款商品数
INSERT OVERWRITE TABLE dw_zaful_recommend.zaful_app_recommend_cartpage_wuc_paid_sku_tmp
SELECT
  SUM(x2.goods_number) AS paid_sku,
  x1.platform,
  x1.af_inner_mediasource,
  x1.language,
  x1.country_code,
  '${ADD_TIME}' as add_time
from
  dw_zaful_recommend.zaful_app_recommend_cartpage_wuc_sku_user_country_tmp x1
  INNER JOIN dw_zaful_recommend.zaful_app_recommend_cartpage_wuc_order_info_good_tmp x2 ON x1.user_id = x2.user_id
  AND x1.goods_sn = x2.goods_sn
where
  x2.order_status not in ('0','11')
group by
  x1.platform,
  x1.af_inner_mediasource,
  x1.language,
  x1.country_code
;


--付款金额
INSERT OVERWRITE TABLE dw_zaful_recommend.zaful_app_recommend_cartpage_wuc_paid_amount_tmp
SELECT
  sum(x2.pay_amount) as paid_amount,
  x1.platform,
  x1.af_inner_mediasource,
  x1.language,
  x1.country_code,
  '${ADD_TIME}' as add_time
from
  dw_zaful_recommend.zaful_app_recommend_cartpage_wuc_sku_user_country_tmp x1
  INNER JOIN dw_zaful_recommend.zaful_app_recommend_cartpage_wuc_order_info_good_tmp x2 ON x1.user_id = x2.user_id
  AND x1.goods_sn = x2.goods_sn
where
  x2.order_status not in ('0','10','11','13')
group by
  x1.platform,
  x1.af_inner_mediasource,
  x1.language,
  x1.country_code
  ;


--商品加收次数	
INSERT OVERWRITE TABLE dw_zaful_recommend.zaful_app_recommend_cartpage_wuc_sku_add_count_tmp
SELECT
  count(event_name) as sku_add_count,
  platform,
  get_json_object(event_value, '$.af_inner_mediasource') AS af_inner_mediasource,
  language,
  country_code,
  '${ADD_TIME}' as add_time
   FROM
  ods.ods_app_burial_log 
  WHERE
  concat_ws('-', year, month, day) = '${ADD_TIME}'
  and event_name='af_add_to_wishlist'
  and get_json_object(event_value, '$.af_inner_mediasource') ='recommend_cartpage'
      and site='zaful'
group by
  platform,
  get_json_object(event_value, '$.af_inner_mediasource'),
  language,
  country_code
  ;



--加收用户数	
INSERT OVERWRITE TABLE dw_zaful_recommend.zaful_app_recommend_cartpage_wuc_user_add_count_tmp
SELECT
  count(distinct appsflyer_device_id) as user_add_count,
  platform,
  get_json_object(event_value, '$.af_inner_mediasource') AS af_inner_mediasource,
  language,
  country_code,
  '${ADD_TIME}' as add_time
   FROM
  ods.ods_app_burial_log 
  WHERE
  concat_ws('-', year, month, day) = '${ADD_TIME}'
  and event_name='af_add_to_wishlist'
  and get_json_object(event_value, '$.af_inner_mediasource') ='recommend_cartpage'
      and site='zaful'
group by
  platform,
  get_json_object(event_value, '$.af_inner_mediasource'),
  language,
  country_code
  ;
  





--GMV
INSERT OVERWRITE TABLE dw_zaful_recommend.zaful_app_recommend_cartpage_wuc_gmv_tmp
SELECT
  sum(x2.pay_amount) as gmv,
  x1.platform,
  x1.af_inner_mediasource,
  x1.language,
  x1.country_code,
  '${ADD_TIME}' as add_time
from
  dw_zaful_recommend.zaful_app_recommend_cartpage_wuc_sku_user_country_tmp x1
  INNER JOIN dw_zaful_recommend.zaful_app_recommend_cartpage_wuc_order_info_good_tmp x2 ON x1.user_id = x2.user_id
  AND x1.goods_sn = x2.goods_sn
group by
  x1.platform,
  x1.af_inner_mediasource,
  x1.language,
  x1.country_code
  ;

--下单客户数
INSERT OVERWRITE TABLE dw_zaful_recommend.zaful_app_recommend_cartpage_wuc_order_user_count_tmp
SELECT
  count(distinct x1.appsflyer_device_id) as order_user_count,
  x1.platform,
  x1.af_inner_mediasource,
  x1.language,
  x1.country_code,
  '${ADD_TIME}' as add_time
from
  dw_zaful_recommend.zaful_app_recommend_cartpage_wuc_device_id_sku_user_country_tmp x1
  INNER JOIN dw_zaful_recommend.zaful_app_recommend_cartpage_wuc_order_info_good_tmp x2 ON x1.user_id = x2.user_id
  AND x1.goods_sn = x2.goods_sn
group by
  x1.platform,
  x1.af_inner_mediasource,
  x1.language,
  x1.country_code
;


--付款客户数
INSERT OVERWRITE TABLE dw_zaful_recommend.zaful_app_recommend_cartpage_wuc_pay_uv_tmp
SELECT
  count(distinct x1.appsflyer_device_id) as pay_uv,
  x1.platform,
  x1.af_inner_mediasource,
  x1.language,
  x1.country_code,
  '${ADD_TIME}' as add_time
from
  dw_zaful_recommend.zaful_app_recommend_cartpage_wuc_device_id_sku_user_country_tmp x1
  INNER JOIN dw_zaful_recommend.zaful_app_recommend_cartpage_wuc_order_info_good_tmp x2 ON x1.user_id = x2.user_id
  AND x1.goods_sn = x2.goods_sn
where
  x2.order_status not in ('0','10','11','13')
group by
  x1.platform,
  x1.af_inner_mediasource,
  x1.language,
  x1.country_code
;











--所有结果汇总
INSERT into TABLE dw_zaful_recommend.zaful_app_recommend_cartpage_wuc_report PARTITION (add_time = '${ADD_TIME}')
select
  NVL(a.pv,0),
  NVL(b.uv,0),
  c.position_exp_num,
  d.position_uv,
  e.exp_num,
  f.click_num,
  g.click_uv,
  f.click_num / e.exp_num *100,
  h.cart_num,
  i.cart_uv,
  h.cart_num / e.exp_num *100,
  i.cart_uv / b.uv *100,
  j.order_sku_num,
  j.order_sku_num / h.cart_num *100,
  k.paid_sku,
  l.paid_amount,
  m.sku_add_count,
  n.user_add_count,
  o.gmv,
  o.gmv / e.exp_num * 1000,
  p.order_user_count,
  q.pay_uv,
  l.paid_amount / q.pay_uv * 100,
  'android',
  a.af_inner_mediasource,
  '购物车页推荐',
  a.language,
  a.country_code

from   
(select pv, add_time,af_inner_mediasource,language,country_code from    dw_zaful_recommend.zaful_app_recommend_cartpage_wuc_pv_tmp where  platform='android') a 
left join
(select uv, add_time,af_inner_mediasource,language,country_code from    dw_zaful_recommend.zaful_app_recommend_cartpage_wuc_uv_tmp where  platform='android') b 
on a.add_time=b.add_time and a.af_inner_mediasource=b.af_inner_mediasource and a.language=b.language and a.country_code=b.country_code
left join 
(select position_exp_num, add_time,af_inner_mediasource,language,country_code from   dw_zaful_recommend.zaful_app_recommend_cartpage_wuc_position_exp_num_tmp where  platform='android') c 
on a.add_time=c.add_time and a.af_inner_mediasource=c.af_inner_mediasource and a.language=c.language and a.country_code=c.country_code
left join 
(select position_uv, add_time,af_inner_mediasource,language,country_code from   dw_zaful_recommend.zaful_app_recommend_cartpage_wuc_position_uv_tmp where  platform='android') d 
on a.add_time=d.add_time and a.af_inner_mediasource=d.af_inner_mediasource and a.language=d.language and a.country_code=d.country_code
left join 
(select exp_num, add_time,af_inner_mediasource,language,country_code from   dw_zaful_recommend.zaful_app_recommend_cartpage_wuc_exp_num_tmp where  platform='android') e 
on a.add_time=e.add_time and a.af_inner_mediasource=e.af_inner_mediasource and a.language=e.language and a.country_code=e.country_code
left join 
(select click_num, add_time,af_inner_mediasource,language,country_code from   dw_zaful_recommend.zaful_app_recommend_cartpage_wuc_click_num_tmp where  platform='android') f 
on a.add_time=f.add_time and a.af_inner_mediasource=f.af_inner_mediasource and a.language=f.language and a.country_code=f.country_code
left join 
(select click_uv, add_time,af_inner_mediasource,language,country_code from   dw_zaful_recommend.zaful_app_recommend_cartpage_wuc_click_uv_tmp where  platform='android') g 
on a.add_time=g.add_time and a.af_inner_mediasource=g.af_inner_mediasource and a.language=g.language and a.country_code=g.country_code
left join 
(select cart_num, add_time,af_inner_mediasource,language,country_code from   dw_zaful_recommend.zaful_app_recommend_cartpage_wuc_cart_num_tmp where  platform='android') h 
on a.add_time=h.add_time and a.af_inner_mediasource=h.af_inner_mediasource and a.language=h.language and a.country_code=h.country_code
left join 
(select cart_uv, add_time,af_inner_mediasource,language,country_code from   dw_zaful_recommend.zaful_app_recommend_cartpage_wuc_cart_uv_tmp where  platform='android') i 
on a.add_time=i.add_time and a.af_inner_mediasource=i.af_inner_mediasource and a.language=i.language and a.country_code=i.country_code
left join 
(select order_sku_num, add_time,af_inner_mediasource,language,country_code from   dw_zaful_recommend.zaful_app_recommend_cartpage_wuc_order_sku_num_tmp where  platform='android') j 
on a.add_time=j.add_time and a.af_inner_mediasource=j.af_inner_mediasource and a.language=j.language and a.country_code=j.country_code
left join 
(select paid_sku, add_time,af_inner_mediasource,language,country_code from   dw_zaful_recommend.zaful_app_recommend_cartpage_wuc_paid_sku_tmp where  platform='android') k 
on a.add_time=k.add_time and a.af_inner_mediasource=k.af_inner_mediasource and a.language=k.language and a.country_code=k.country_code
left join 
(select paid_amount, add_time,af_inner_mediasource,language,country_code from   dw_zaful_recommend.zaful_app_recommend_cartpage_wuc_paid_amount_tmp where  platform='android') l
on a.add_time=l.add_time and a.af_inner_mediasource=l.af_inner_mediasource and a.language=l.language and a.country_code=l.country_code
left join 
(select sku_add_count, add_time,af_inner_mediasource,language,country_code from   dw_zaful_recommend.zaful_app_recommend_cartpage_wuc_sku_add_count_tmp where  platform='android') m
on a.add_time=m.add_time and a.af_inner_mediasource=m.af_inner_mediasource and a.language=m.language and a.country_code=m.country_code
left join 
(select user_add_count, add_time,af_inner_mediasource,language,country_code from   dw_zaful_recommend.zaful_app_recommend_cartpage_wuc_user_add_count_tmp where  platform='android') n
on a.add_time=n.add_time and a.af_inner_mediasource=n.af_inner_mediasource and a.language=n.language and a.country_code=n.country_code
left join 
(select gmv, add_time,af_inner_mediasource,language,country_code from   dw_zaful_recommend.zaful_app_recommend_cartpage_wuc_gmv_tmp where  platform='android') o
on a.add_time=o.add_time and a.af_inner_mediasource=o.af_inner_mediasource and a.language=o.language and a.country_code=o.country_code
left join 
(select order_user_count, add_time,af_inner_mediasource,language,country_code from   dw_zaful_recommend.zaful_app_recommend_cartpage_wuc_order_user_count_tmp where  platform='android') p
on a.add_time=p.add_time and a.af_inner_mediasource=p.af_inner_mediasource and a.language=p.language and a.country_code=p.country_code
left join 
(select pay_uv, add_time,af_inner_mediasource,language,country_code from   dw_zaful_recommend.zaful_app_recommend_cartpage_wuc_pay_uv_tmp where  platform='android') q
on a.add_time=p.add_time and a.af_inner_mediasource=p.af_inner_mediasource and a.language=p.language and a.country_code=p.country_code

union all

select
  NVL(a.pv,0),
  NVL(b.uv,0),
  c.position_exp_num,
  d.position_uv,
  e.exp_num,
  f.click_num,
  g.click_uv,
  f.click_num / e.exp_num *100,
  h.cart_num,
  i.cart_uv,
  h.cart_num / e.exp_num *100,
  i.cart_uv / b.uv *100,
  j.order_sku_num,
  j.order_sku_num / h.cart_num *100,
  k.paid_sku,
  l.paid_amount,
  m.sku_add_count,
  n.user_add_count,
  o.gmv,
  o.gmv / e.exp_num * 1000,
  p.order_user_count,
  q.pay_uv,
  l.paid_amount / q.pay_uv * 100,
  'ios',
  a.af_inner_mediasource,
  '购物车页推荐',
  a.language,
  a.country_code

from   
(select pv, add_time,af_inner_mediasource,language,country_code from    dw_zaful_recommend.zaful_app_recommend_cartpage_wuc_pv_tmp where  platform='ios') a 
left join
(select uv, add_time,af_inner_mediasource,language,country_code from    dw_zaful_recommend.zaful_app_recommend_cartpage_wuc_uv_tmp where  platform='ios') b 
on a.add_time=b.add_time and a.af_inner_mediasource=b.af_inner_mediasource and a.language=b.language and a.country_code=b.country_code
left join 
(select position_exp_num, add_time,af_inner_mediasource,language,country_code from   dw_zaful_recommend.zaful_app_recommend_cartpage_wuc_position_exp_num_tmp where  platform='ios') c 
on a.add_time=c.add_time and a.af_inner_mediasource=c.af_inner_mediasource and a.language=c.language and a.country_code=c.country_code
left join 
(select position_uv, add_time,af_inner_mediasource,language,country_code from   dw_zaful_recommend.zaful_app_recommend_cartpage_wuc_position_uv_tmp where  platform='ios') d 
on a.add_time=d.add_time and a.af_inner_mediasource=d.af_inner_mediasource and a.language=d.language and a.country_code=d.country_code
left join 
(select exp_num, add_time,af_inner_mediasource,language,country_code from   dw_zaful_recommend.zaful_app_recommend_cartpage_wuc_exp_num_tmp where  platform='ios') e 
on a.add_time=e.add_time and a.af_inner_mediasource=e.af_inner_mediasource and a.language=e.language and a.country_code=e.country_code
left join 
(select click_num, add_time,af_inner_mediasource,language,country_code from   dw_zaful_recommend.zaful_app_recommend_cartpage_wuc_click_num_tmp where  platform='ios') f 
on a.add_time=f.add_time and a.af_inner_mediasource=f.af_inner_mediasource and a.language=f.language and a.country_code=f.country_code
left join 
(select click_uv, add_time,af_inner_mediasource,language,country_code from   dw_zaful_recommend.zaful_app_recommend_cartpage_wuc_click_uv_tmp where  platform='ios') g 
on a.add_time=g.add_time and a.af_inner_mediasource=g.af_inner_mediasource and a.language=g.language and a.country_code=g.country_code
left join 
(select cart_num, add_time,af_inner_mediasource,language,country_code from   dw_zaful_recommend.zaful_app_recommend_cartpage_wuc_cart_num_tmp where  platform='ios') h 
on a.add_time=h.add_time and a.af_inner_mediasource=h.af_inner_mediasource and a.language=h.language and a.country_code=h.country_code
left join 
(select cart_uv, add_time,af_inner_mediasource,language,country_code from   dw_zaful_recommend.zaful_app_recommend_cartpage_wuc_cart_uv_tmp where  platform='ios') i 
on a.add_time=i.add_time and a.af_inner_mediasource=i.af_inner_mediasource and a.language=i.language and a.country_code=i.country_code
left join 
(select order_sku_num, add_time,af_inner_mediasource,language,country_code from   dw_zaful_recommend.zaful_app_recommend_cartpage_wuc_order_sku_num_tmp where  platform='ios') j 
on a.add_time=j.add_time and a.af_inner_mediasource=j.af_inner_mediasource and a.language=j.language and a.country_code=j.country_code
left join 
(select paid_sku, add_time,af_inner_mediasource,language,country_code from   dw_zaful_recommend.zaful_app_recommend_cartpage_wuc_paid_sku_tmp where  platform='ios') k 
on a.add_time=k.add_time and a.af_inner_mediasource=k.af_inner_mediasource and a.language=k.language and a.country_code=k.country_code
left join 
(select paid_amount, add_time,af_inner_mediasource,language,country_code from   dw_zaful_recommend.zaful_app_recommend_cartpage_wuc_paid_amount_tmp where  platform='ios') l
on a.add_time=l.add_time and a.af_inner_mediasource=l.af_inner_mediasource and a.language=l.language and a.country_code=l.country_code
left join 
(select sku_add_count, add_time,af_inner_mediasource,language,country_code from   dw_zaful_recommend.zaful_app_recommend_cartpage_wuc_sku_add_count_tmp where  platform='ios') m
on a.add_time=m.add_time and a.af_inner_mediasource=m.af_inner_mediasource and a.language=m.language and a.country_code=m.country_code
left join 
(select user_add_count, add_time,af_inner_mediasource,language,country_code from   dw_zaful_recommend.zaful_app_recommend_cartpage_wuc_user_add_count_tmp where  platform='ios') n
on a.add_time=n.add_time and a.af_inner_mediasource=n.af_inner_mediasource and a.language=n.language and a.country_code=n.country_code
left join 
(select gmv, add_time,af_inner_mediasource,language,country_code from   dw_zaful_recommend.zaful_app_recommend_cartpage_wuc_gmv_tmp where  platform='ios') o
on a.add_time=o.add_time and a.af_inner_mediasource=o.af_inner_mediasource and a.language=o.language and a.country_code=o.country_code
left join 
(select order_user_count, add_time,af_inner_mediasource,language,country_code from   dw_zaful_recommend.zaful_app_recommend_cartpage_wuc_order_user_count_tmp where  platform='ios') p
on a.add_time=p.add_time and a.af_inner_mediasource=p.af_inner_mediasource and a.language=p.language and a.country_code=p.country_code
left join 
(select pay_uv, add_time,af_inner_mediasource,language,country_code from   dw_zaful_recommend.zaful_app_recommend_cartpage_wuc_pay_uv_tmp where  platform='ios') q
on a.add_time=p.add_time and a.af_inner_mediasource=p.af_inner_mediasource and a.language=p.language and a.country_code=p.country_code
;



--导出数据表
INSERT OVERWRITE TABLE dw_zaful_recommend.zaful_app_recommend_cartpage_wuc_report_exp
select 
x.pv,
x.uv,
x.position_exp_num,
x.position_uv,
x.exp_num,
x.click_num,
x.click_uv,
x.sku_click_ratio,
x.cart_num,
x.cart_uv,
x.sku_cart_ratio,
x.user_cart_ratio,
x.order_sku_num,
x.order_sku_radio,
x.paid_sku,
x.paid_amount,
x.sku_add_count,
x.user_add_count,
x.gmv,
x.gmv_cost_mille,
x.order_user_count,
x.pay_uv,
x.paid_user,
x.platform,
x.recommend_position,
x.position_name,
x.language,
y.country,
x.add_time 
from 
(
select
  *
from 
  dw_zaful_recommend.zaful_app_recommend_cartpage_wuc_report
where 
  add_time = '${ADD_TIME}'
) x 
left join dw_zaful_recommend.cuntryode y on x.country_code=y.code
;













































































































































