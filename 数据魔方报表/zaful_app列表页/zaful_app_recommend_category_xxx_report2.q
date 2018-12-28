
--@author wuchao
--@date 2018年12月11日 
--@desc  Zaful App列表页报表新需求chongxie


SET mapred.job.name=zaful_app_category_xxx_report2;
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
CREATE TABLE IF NOT EXISTS dw_zaful_recommend.zaful_app_category_xxx_report(
pv                        INT            COMMENT "页面PV",
uv                        INT            COMMENT "页面UV",
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
gmv                       INT            COMMENT "gmv",
purchase_num              INT            COMMENT "销量",  
pay_uv                    INT            COMMENT "付款uv",
pay_amount                INT            COMMENT "销售额",
sku_purchase_ratio        decimal(10,4)  COMMENT "商品购买转化率",
user_purchase_ratio       decimal(10,4)  COMMENT "用户购买转化率",
gmv_cost_mille            decimal(10,4)  COMMENT "千次曝光GMV",
collect_uv                INT            COMMENT "商品收藏UV",
collect_num               INT            COMMENT "商品收藏数",
platform                  STRING         COMMENT "平台",
recommend_position        STRING         COMMENT "列表页编号",
position_name             STRING         COMMENT "列表页名称",
language                    STRING         COMMENT "语言站",
country_code                   STRING         COMMENT "国家"
)
COMMENT 'zaful APP列表页数据报表新需求重写'
PARTITIONED BY (add_time STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\u0001' LINES TERMINATED BY '\n' STORED AS TEXTFILE
;

--输出结果表导出表
CREATE TABLE IF NOT EXISTS dw_zaful_recommend.zaful_app_category_xxx_report_exp(
pv                        INT            COMMENT "页面PV",
uv                        INT            COMMENT "页面UV",
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
gmv                       INT            COMMENT "gmv",
purchase_num              INT            COMMENT "销量",  
pay_uv                    INT            COMMENT "付款uv",
pay_amount                INT            COMMENT "销售额",
sku_purchase_ratio        decimal(10,4)  COMMENT "商品购买转化率",
user_purchase_ratio       decimal(10,4)  COMMENT "用户购买转化率",
gmv_cost_mille            decimal(10,4)  COMMENT "千次曝光GMV",
collect_uv                INT            COMMENT "商品收藏UV",
collect_num               INT            COMMENT "商品收藏数",
platform                  STRING         COMMENT "平台",
recommend_position        STRING         COMMENT "列表页编号",
position_name             STRING         COMMENT "列表页名称",
language                    STRING         COMMENT "语言站",
country_code                   STRING         COMMENT "国家",
add_time                  STRING         comment ""
)
;


--页面PV 
CREATE TABLE IF NOT EXISTS dw_zaful_recommend.zaful_app_category_xxx_pv_tmp(
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
CREATE TABLE IF NOT EXISTS dw_zaful_recommend.zaful_app_category_xxx_uv_tmp(
uv                        INT            COMMENT "页面UV",
platform                  STRING         COMMENT "平台",
af_inner_mediasource      STRING         COMMENT "列表页",
language                    STRING         COMMENT "语言站",
country_code                   STRING         COMMENT "国家",
add_time                  STRING         COMMENT "日期"
)
COMMENT "列表页报表页面UV中间表"
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\u0001' LINES TERMINATED BY '\n' STORED AS TEXTFILE;



--商品曝光数	
CREATE TABLE IF NOT EXISTS dw_zaful_recommend.zaful_app_category_xxx_exp_num_tmp(
exp_num                   INT            COMMENT "商品曝光数",
platform                  STRING         COMMENT "平台",
af_inner_mediasource      STRING         COMMENT "列表页",
language                    STRING         COMMENT "语言站",
country_code                   STRING         COMMENT "国家",
add_time                  STRING         COMMENT "日期"
)
COMMENT "列表页报表商品曝光数中间表"
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\u0001' LINES TERMINATED BY '\n' STORED AS TEXTFILE;


--查看商品UV	
CREATE TABLE IF NOT EXISTS dw_zaful_recommend.zaful_app_category_xxx_sku_uv_tmp(
sku_uv                    INT            COMMENT "查看商品UV",
platform                  STRING         COMMENT "平台",
af_inner_mediasource      STRING         COMMENT "列表页",
language                    STRING         COMMENT "语言站",
country_code                   STRING         COMMENT "国家",
add_time                  STRING         COMMENT "日期"
)
COMMENT "列表页报表查看商品UV中间表"
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\u0001' LINES TERMINATED BY '\n' STORED AS TEXTFILE;


--商品点击数	
CREATE TABLE IF NOT EXISTS dw_zaful_recommend.zaful_app_category_xxx_click_num_tmp(
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
CREATE TABLE IF NOT EXISTS dw_zaful_recommend.zaful_app_category_xxx_click_uv_tmp(
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
CREATE TABLE IF NOT EXISTS dw_zaful_recommend.zaful_app_category_xxx_cart_num_tmp(
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
CREATE TABLE IF NOT EXISTS dw_zaful_recommend.zaful_app_category_xxx_cart_uv_tmp(
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
CREATE TABLE IF NOT EXISTS dw_zaful_recommend.zaful_app_category_xxx_sku_user_country_tmp(
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
CREATE TABLE IF NOT EXISTS dw_zaful_recommend.zaful_app_category_xxx_device_id_sku_user_country_tmp(
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



CREATE TABLE IF NOT EXISTS dw_zaful_recommend.zaful_app_category_xxx_order_info_good_tmp(
goods_sn                  STRING         COMMENT "sku",
user_id                   STRING         COMMENT "用户ID",
order_status              STRING         COMMENT "订单状态",
pay_amount                decimal(10,2)  COMMENT "购买金额",
goods_number              STRING         COMMENT "商品数量"
)
COMMENT "列表页报表user-sku中间表"
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\u0001' LINES TERMINATED BY '\n' STORED AS TEXTFILE;


--下单商品数结果表
CREATE TABLE IF NOT EXISTS dw_zaful_recommend.zaful_app_category_xxx_order_sku_num_tmp(
order_sku_num                 INT        COMMENT "下单商品数",
platform                  STRING         COMMENT "平台",
af_inner_mediasource      STRING         COMMENT "列表页",
language                    STRING         COMMENT "语言站",
country_code                   STRING         COMMENT "国家",
add_time                  STRING         COMMENT "日期"
)
COMMENT "列表页报表下单商品数中间表"
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\u0001' LINES TERMINATED BY '\n' STORED AS TEXTFILE;


--下单uv
CREATE TABLE IF NOT EXISTS dw_zaful_recommend.zaful_app_category_xxx_order_uv_tmp(
order_uv                  INT            COMMENT "下单UV",
platform                  STRING         COMMENT "平台",
af_inner_mediasource      STRING         COMMENT "列表页",
language                    STRING         COMMENT "语言站",
country_code                   STRING         COMMENT "国家",
add_time                  STRING         COMMENT "日期"
)
COMMENT "列表页报表下单uv中间表"
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\u0001' LINES TERMINATED BY '\n' STORED AS TEXTFILE;

--GMV
CREATE TABLE IF NOT EXISTS dw_zaful_recommend.zaful_app_category_xxx_gmv_tmp(
gmv              INT            COMMENT "gmv",
platform                  STRING         COMMENT "平台",
af_inner_mediasource      STRING         COMMENT "列表页",
language                    STRING         COMMENT "语言站",
country_code                   STRING         COMMENT "国家",
add_time                  STRING         COMMENT "日期"
)
COMMENT "列表页报表销量中间表"
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\u0001' LINES TERMINATED BY '\n' STORED AS TEXTFILE;

--销量
CREATE TABLE IF NOT EXISTS dw_zaful_recommend.zaful_app_category_xxx_purchase_num_tmp(
purchase_num              INT            COMMENT "销量",
platform                  STRING         COMMENT "平台",
af_inner_mediasource      STRING         COMMENT "列表页",
language                    STRING         COMMENT "语言站",
country_code                   STRING         COMMENT "国家",
add_time                  STRING         COMMENT "日期"
)
COMMENT "列表页报表销量中间表"
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\u0001' LINES TERMINATED BY '\n' STORED AS TEXTFILE;



--付款uv
CREATE TABLE IF NOT EXISTS dw_zaful_recommend.zaful_app_category_xxx_pay_uv_tmp(
pay_uv                    INT            COMMENT "付款uv",
platform                  STRING         COMMENT "平台",
af_inner_mediasource      STRING         COMMENT "列表页",
language                    STRING         COMMENT "语言站",
country_code                   STRING         COMMENT "国家",
add_time                  STRING         COMMENT "日期"
)
COMMENT "列表页报表付款uv中间表"
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\u0001' LINES TERMINATED BY '\n' STORED AS TEXTFILE;


--销售额
CREATE TABLE IF NOT EXISTS dw_zaful_recommend.zaful_app_category_xxx_pay_amount_tmp(
pay_amount                decimal(10,2)  COMMENT "购买金额",
platform                  STRING         COMMENT "平台",
af_inner_mediasource      STRING         COMMENT "列表页",
language                    STRING         COMMENT "语言站",
country_code                   STRING         COMMENT "国家",
add_time                  STRING         COMMENT "日期"
)
COMMENT "列表页报表购买金额中间表"
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\u0001' LINES TERMINATED BY '\n' STORED AS TEXTFILE;


--商品收藏UV	
CREATE TABLE IF NOT EXISTS dw_zaful_recommend.zaful_app_category_xxx_collect_uv_tmp(
collect_uv                INT            COMMENT "商品收藏UV",
platform                  STRING         COMMENT "平台",
af_inner_mediasource      STRING         COMMENT "列表页",
language                    STRING         COMMENT "语言站",
country_code                   STRING         COMMENT "国家",
add_time                  STRING         COMMENT "日期"
)
COMMENT "列表页报表商品收藏UV中间表"
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\u0001' LINES TERMINATED BY '\n' STORED AS TEXTFILE;

--商品收藏数	
CREATE TABLE IF NOT EXISTS dw_zaful_recommend.zaful_app_category_xxx_collect_num_tmp(
collect_num               INT            COMMENT "商品收藏数",
platform                  STRING         COMMENT "平台",
af_inner_mediasource      STRING         COMMENT "列表页",
language                    STRING         COMMENT "语言站",
country_code                   STRING         COMMENT "国家",
add_time                  STRING         COMMENT "日期"
)
COMMENT "列表页报表商品收藏数中间表"
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
INSERT OVERWRITE TABLE dw_zaful_recommend.zaful_app_category_xxx_sku_user_country_tmp
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
      and get_json_object(event_value, '$.af_inner_mediasource')  like 'category_%'
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
INSERT OVERWRITE TABLE dw_zaful_recommend.zaful_app_category_xxx_device_id_sku_user_country_tmp
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
      and get_json_object(event_value, '$.af_inner_mediasource')  like 'category_%'
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
INSERT OVERWRITE TABLE dw_zaful_recommend.zaful_app_category_xxx_order_info_good_tmp
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
INSERT OVERWRITE TABLE dw_zaful_recommend.zaful_app_category_xxx_pv_tmp
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
      get_json_object(event_value, '$.af_inner_mediasource') as af_inner_mediasource,
      language,
      country_code
    FROM
      ods.ods_app_burial_log 
    WHERE
      concat_ws('-', year, month, day) = '${ADD_TIME}'
        and event_name='af_impression'
        and get_json_object(event_value, '$.af_inner_mediasource')  like 'category_%'
        and site='zaful'
  ) m
group by
  m.platform,
  m.af_inner_mediasource,
  m.language,
  m.country_code
  ;


--页面UV
INSERT OVERWRITE TABLE dw_zaful_recommend.zaful_app_category_xxx_uv_tmp
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
      get_json_object(event_value, '$.af_inner_mediasource') as af_inner_mediasource,
      language,
      country_code
    FROM
      ods.ods_app_burial_log 
    WHERE
      concat_ws('-', year, month, day) = '${ADD_TIME}'
        and event_name='af_impression'
        and get_json_object(event_value, '$.af_inner_mediasource')  like 'category_%'
        and site='zaful'
  ) m
group by
  m.platform,
  m.af_inner_mediasource,
  m.language,
  m.country_code
  ;

--商品曝光数
INSERT OVERWRITE TABLE dw_zaful_recommend.zaful_app_category_xxx_exp_num_tmp
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
  and get_json_object(event_value, '$.af_inner_mediasource')  like 'category_%'
      and site='zaful'
) m 
group by
m.platform,
m.af_inner_mediasource,
m.language,
m.country_code
;


--查看商品UV
INSERT OVERWRITE TABLE dw_zaful_recommend.zaful_app_category_xxx_sku_uv_tmp
SELECT
  count(distinct m.appsflyer_device_id) AS sku_uv,
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
      get_json_object(event_value, '$.af_inner_mediasource')  as af_inner_mediasource,
      language,
      country_code
    FROM
      ods.ods_app_burial_log 
    WHERE
      concat_ws('-', year, month, day) = '${ADD_TIME}'
        and event_name='af_impression'
		    and get_json_object(event_value, '$.af_inner_mediasource')  like 'category_%'
        and site='zaful'
  ) m
group by
  m.platform,
  m.af_inner_mediasource,
  m.language,
  m.country_code
  ;


--商品点击数
INSERT OVERWRITE TABLE dw_zaful_recommend.zaful_app_category_xxx_click_num_tmp
SELECT
  count(event_name) AS click_num,
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
  and get_json_object(event_value, '$.af_inner_mediasource')  like 'category_%'
      and site='zaful'
group by
  platform,
  get_json_object(event_value, '$.af_inner_mediasource'),
  language,
  country_code
;


--点击UV
INSERT OVERWRITE TABLE dw_zaful_recommend.zaful_app_category_xxx_click_uv_tmp
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
		and get_json_object(event_value, '$.af_inner_mediasource')  like 'category_%'
        and site='zaful'

  ) m
group by
  m.platform,
  m.af_inner_mediasource,
  m.language,
  m.country_code
  ;

  
  
--商品加购数
INSERT OVERWRITE TABLE dw_zaful_recommend.zaful_app_category_xxx_cart_num_tmp
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
  and get_json_object(event_value, '$.af_inner_mediasource')  like 'category_%'
      and site='zaful'
) m 
group by
m.platform,
m.af_inner_mediasource,
m.language,
m.country_code
;

--加购UV
INSERT OVERWRITE TABLE dw_zaful_recommend.zaful_app_category_xxx_cart_uv_tmp
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
		    and get_json_object(event_value, '$.af_inner_mediasource')  like 'category_%'
        and site='zaful'
  ) m
group by
  m.platform,
  m.af_inner_mediasource,
  m.language,
  m.country_code
  ;


--下单商品数
INSERT OVERWRITE TABLE dw_zaful_recommend.zaful_app_category_xxx_order_sku_num_tmp
SELECT
  SUM(x2.goods_number) AS order_num,
  x1.platform,
  x1.af_inner_mediasource,
  x1.language,
  x1.country_code,
  '${ADD_TIME}' as add_time
from
  dw_zaful_recommend.zaful_app_category_xxx_sku_user_country_tmp x1
  INNER JOIN dw_zaful_recommend.zaful_app_category_xxx_order_info_good_tmp x2 ON x1.user_id = x2.user_id
  AND x1.goods_sn = x2.goods_sn
group by
  x1.platform,
  x1.af_inner_mediasource,
  x1.language,
  x1.country_code
;


--下单UV
INSERT OVERWRITE TABLE dw_zaful_recommend.zaful_app_category_xxx_order_uv_tmp
SELECT
  count(distinct x1.appsflyer_device_id) as order_uv,
  x1.platform,
  x1.af_inner_mediasource,
  x1.language,
  x1.country_code,
  '${ADD_TIME}' as add_time
from
  dw_zaful_recommend.zaful_app_category_xxx_device_id_sku_user_country_tmp x1
  INNER JOIN dw_zaful_recommend.zaful_app_category_xxx_order_info_good_tmp x2 ON x1.user_id = x2.user_id
  AND x1.goods_sn = x2.goods_sn
group by
  x1.platform,
  x1.af_inner_mediasource,
  x1.language,
  x1.country_code
;

--GMV
INSERT OVERWRITE TABLE dw_zaful_recommend.zaful_app_category_xxx_gmv_tmp
SELECT
  sum(x2.pay_amount) as gmv,
  x1.platform,
  x1.af_inner_mediasource,
  x1.language,
  x1.country_code,
  '${ADD_TIME}' as add_time
from
  dw_zaful_recommend.zaful_app_category_xxx_sku_user_country_tmp x1
  INNER JOIN dw_zaful_recommend.zaful_app_category_xxx_order_info_good_tmp x2 ON x1.user_id = x2.user_id
  AND x1.goods_sn = x2.goods_sn
group by
  x1.platform,
  x1.af_inner_mediasource,
  x1.language,
  x1.country_code
  ;

--销量
INSERT OVERWRITE TABLE dw_zaful_recommend.zaful_app_category_xxx_purchase_num_tmp
SELECT
  SUM(x2.goods_number) AS purchase_num,
  x1.platform,
  x1.af_inner_mediasource,
  x1.language,
  x1.country_code,
  '${ADD_TIME}' as add_time
from
  dw_zaful_recommend.zaful_app_category_xxx_sku_user_country_tmp x1
  INNER JOIN dw_zaful_recommend.zaful_app_category_xxx_order_info_good_tmp x2 ON x1.user_id = x2.user_id
  AND x1.goods_sn = x2.goods_sn
where
  x2.order_status not in ('0','10','11','13')
group by
  x1.platform,
  x1.af_inner_mediasource,
  x1.language,
  x1.country_code
;


--付款UV
INSERT OVERWRITE TABLE dw_zaful_recommend.zaful_app_category_xxx_pay_uv_tmp
SELECT
  count(distinct x1.appsflyer_device_id) as pay_uv,
  x1.platform,
  x1.af_inner_mediasource,
  x1.language,
  x1.country_code,
  '${ADD_TIME}' as add_time
from
  dw_zaful_recommend.zaful_app_category_xxx_device_id_sku_user_country_tmp x1
  INNER JOIN dw_zaful_recommend.zaful_app_category_xxx_order_info_good_tmp x2 ON x1.user_id = x2.user_id
  AND x1.goods_sn = x2.goods_sn
where
  x2.order_status not in ('0','10','11','13')
group by
  x1.platform,
  x1.af_inner_mediasource,
  x1.language,
  x1.country_code
;



--销售额
INSERT OVERWRITE TABLE dw_zaful_recommend.zaful_app_category_xxx_pay_amount_tmp
SELECT
  sum(x2.pay_amount) as pay_amount,
  x1.platform,
  x1.af_inner_mediasource,
  x1.language,
  x1.country_code,
  '${ADD_TIME}' as add_time
from
  dw_zaful_recommend.zaful_app_category_xxx_sku_user_country_tmp x1
  INNER JOIN dw_zaful_recommend.zaful_app_category_xxx_order_info_good_tmp x2 ON x1.user_id = x2.user_id
  AND x1.goods_sn = x2.goods_sn
where
  x2.order_status not in ('0','10','11','13')
group by
  x1.platform,
  x1.af_inner_mediasource,
  x1.language,
  x1.country_code
  ;


--商品收藏UV	
INSERT OVERWRITE TABLE dw_zaful_recommend.zaful_app_category_xxx_collect_uv_tmp
SELECT
  count(distinct appsflyer_device_id) as collect_uv,
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
  and get_json_object(event_value, '$.af_inner_mediasource')  like 'category_%'
      and site='zaful'
group by
  platform,
  get_json_object(event_value, '$.af_inner_mediasource'),
  language,
  country_code
  ;
  

--商品收藏数	
INSERT OVERWRITE TABLE dw_zaful_recommend.zaful_app_category_xxx_collect_num_tmp
SELECT
  count(*) as collect_num,
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
  and get_json_object(event_value, '$.af_inner_mediasource')  like 'category_%'
      and site='zaful'
group by
  platform,
  get_json_object(event_value, '$.af_inner_mediasource'),
  language,
  country_code
  ;




--所有结果汇总
INSERT OVERWRITE TABLE dw_zaful_recommend.zaful_app_category_xxx_report PARTITION (add_time = '${ADD_TIME}')
select
  NVL(a.pv,0),
  NVL(b.uv,0),
  c.exp_num,
  d.sku_uv,
  e.click_num,
  f.click_uv,
  e.click_num / c.exp_num * 100,
  f.click_uv / d.sku_uv * 100,
  g.cart_num,
  h.cart_uv,
  g.cart_num / c.exp_num * 100,
  h.cart_uv / d.sku_uv *100,
  i.order_sku_num,
  j.order_uv,
  i.order_sku_num / c.exp_num * 100,
  j.order_uv / d.sku_uv * 100,
  k.gmv,
  l.purchase_num,
  m.pay_uv,
  n.pay_amount,
  l.purchase_num / c.exp_num * 100,
  m.pay_uv / d.sku_uv * 100,
  k.gmv / c.exp_num * 1000,
  o.collect_uv,
  p.collect_num,
  'android',
  a.af_inner_mediasource,
  '列表页推荐',
  a.language,
  a.country_code

from   
(select pv, add_time,af_inner_mediasource,language,country_code from  dw_zaful_recommend.zaful_app_category_xxx_pv_tmp where  platform='android') a 
left join
(select uv, add_time,af_inner_mediasource,language,country_code from dw_zaful_recommend.zaful_app_category_xxx_uv_tmp where  platform='android') b 
on a.add_time=b.add_time and a.af_inner_mediasource=b.af_inner_mediasource and a.language=b.language and a.country_code=b.country_code
left join 
(select exp_num, add_time,af_inner_mediasource,language,country_code from dw_zaful_recommend.zaful_app_category_xxx_exp_num_tmp where  platform='android') c 
on a.add_time=c.add_time and a.af_inner_mediasource=c.af_inner_mediasource and a.language=c.language and a.country_code=c.country_code
left join 
(select sku_uv, add_time,af_inner_mediasource,language,country_code from   dw_zaful_recommend.zaful_app_category_xxx_sku_uv_tmp where  platform='android') d 
on a.add_time=d.add_time and a.af_inner_mediasource=d.af_inner_mediasource and a.language=d.language and a.country_code=d.country_code
left join 
(select click_num, add_time,af_inner_mediasource,language,country_code from   dw_zaful_recommend.zaful_app_category_xxx_click_num_tmp where  platform='android') e 
on a.add_time=e.add_time and a.af_inner_mediasource=e.af_inner_mediasource and a.language=e.language and a.country_code=e.country_code
left join 
(select click_uv, add_time,af_inner_mediasource,language,country_code from   dw_zaful_recommend.zaful_app_category_xxx_click_uv_tmp where  platform='android') f 
on a.add_time=f.add_time and a.af_inner_mediasource=f.af_inner_mediasource and a.language=f.language and a.country_code=f.country_code
left join 
(select cart_num, add_time,af_inner_mediasource,language,country_code from   dw_zaful_recommend.zaful_app_category_xxx_cart_num_tmp where  platform='android') g 
on a.add_time=g.add_time and a.af_inner_mediasource=g.af_inner_mediasource and a.language=g.language and a.country_code=g.country_code
left join 
(select cart_uv, add_time,af_inner_mediasource,language,country_code from   dw_zaful_recommend.zaful_app_category_xxx_cart_uv_tmp where  platform='android') h 
on a.add_time=h.add_time and a.af_inner_mediasource=h.af_inner_mediasource and a.language=h.language and a.country_code=h.country_code
left join 
(select order_sku_num, add_time,af_inner_mediasource,language,country_code from   dw_zaful_recommend.zaful_app_category_xxx_order_sku_num_tmp where  platform='android') i 
on a.add_time=i.add_time and a.af_inner_mediasource=i.af_inner_mediasource and a.language=i.language and a.country_code=i.country_code
left join 
(select order_uv, add_time,af_inner_mediasource,language,country_code from   dw_zaful_recommend.zaful_app_category_xxx_order_uv_tmp where  platform='android') j 
on a.add_time=j.add_time and a.af_inner_mediasource=j.af_inner_mediasource and a.language=j.language and a.country_code=j.country_code
left join 
(select gmv, add_time,af_inner_mediasource,language,country_code from   dw_zaful_recommend.zaful_app_category_xxx_gmv_tmp where  platform='android') k 
on a.add_time=k.add_time and a.af_inner_mediasource=k.af_inner_mediasource and a.language=k.language and a.country_code=k.country_code
left join 
(select purchase_num, add_time,af_inner_mediasource,language,country_code from   dw_zaful_recommend.zaful_app_category_xxx_purchase_num_tmp where  platform='android') l
on a.add_time=l.add_time and a.af_inner_mediasource=l.af_inner_mediasource and a.language=l.language and a.country_code=l.country_code
left join 
(select pay_uv, add_time,af_inner_mediasource,language,country_code from   dw_zaful_recommend.zaful_app_category_xxx_pay_uv_tmp where  platform='android') m
on a.add_time=m.add_time and a.af_inner_mediasource=m.af_inner_mediasource and a.language=m.language and a.country_code=m.country_code
left join 
(select pay_amount, add_time,af_inner_mediasource,language,country_code from   dw_zaful_recommend.zaful_app_category_xxx_pay_amount_tmp where  platform='android') n
on a.add_time=n.add_time and a.af_inner_mediasource=n.af_inner_mediasource and a.language=n.language and a.country_code=n.country_code
left join 
(select collect_uv, add_time,af_inner_mediasource,language,country_code from   dw_zaful_recommend.zaful_app_category_xxx_collect_uv_tmp where  platform='android') o
on a.add_time=o.add_time and a.af_inner_mediasource=o.af_inner_mediasource and a.language=o.language and a.country_code=o.country_code
left join 
(select collect_num, add_time,af_inner_mediasource,language,country_code from   dw_zaful_recommend.zaful_app_category_xxx_collect_num_tmp where  platform='android') p
on a.add_time=p.add_time and a.af_inner_mediasource=p.af_inner_mediasource and a.language=p.language and a.country_code=p.country_code

union all

select
  NVL(a.pv,0),
  NVL(b.uv,0),
  c.exp_num,
  d.sku_uv,
  e.click_num,
  f.click_uv,
  e.click_num / c.exp_num * 100,
  f.click_uv / d.sku_uv * 100,
  g.cart_num,
  h.cart_uv,
  g.cart_num / c.exp_num * 100,
  h.cart_uv / d.sku_uv *100,
  i.order_sku_num,
  j.order_uv,
  i.order_sku_num / c.exp_num * 100,
  j.order_uv / d.sku_uv * 100,
  k.gmv,
  l.purchase_num,
  m.pay_uv,
  n.pay_amount,
  l.purchase_num / c.exp_num * 100,
  m.pay_uv / d.sku_uv * 100,
  k.gmv / c.exp_num * 1000,
  o.collect_uv,
  p.collect_num,
  'ios',
  a.af_inner_mediasource,
  '列表页推荐',
  a.language,
  a.country_code

from   
(select pv, add_time,af_inner_mediasource,language,country_code from    dw_zaful_recommend.zaful_app_category_xxx_pv_tmp where  platform='ios') a 
left join
(select uv, add_time,af_inner_mediasource,language,country_code from    dw_zaful_recommend.zaful_app_category_xxx_uv_tmp where  platform='ios') b 
on a.add_time=b.add_time and a.af_inner_mediasource=b.af_inner_mediasource and a.language=b.language and a.country_code=b.country_code
left join 
(select exp_num, add_time,af_inner_mediasource,language,country_code from   dw_zaful_recommend.zaful_app_category_xxx_exp_num_tmp where  platform='ios') c 
on a.add_time=c.add_time and a.af_inner_mediasource=c.af_inner_mediasource and a.language=c.language and a.country_code=c.country_code
left join 
(select sku_uv, add_time,af_inner_mediasource,language,country_code from   dw_zaful_recommend.zaful_app_category_xxx_sku_uv_tmp where  platform='ios') d 
on a.add_time=d.add_time and a.af_inner_mediasource=d.af_inner_mediasource and a.language=d.language and a.country_code=d.country_code
left join 
(select click_num, add_time,af_inner_mediasource,language,country_code from   dw_zaful_recommend.zaful_app_category_xxx_click_num_tmp where  platform='ios') e 
on a.add_time=e.add_time and a.af_inner_mediasource=e.af_inner_mediasource and a.language=e.language and a.country_code=e.country_code
left join 
(select click_uv, add_time,af_inner_mediasource,language,country_code from   dw_zaful_recommend.zaful_app_category_xxx_click_uv_tmp where  platform='ios') f 
on a.add_time=f.add_time and a.af_inner_mediasource=f.af_inner_mediasource and a.language=f.language and a.country_code=f.country_code
left join 
(select cart_num, add_time,af_inner_mediasource,language,country_code from   dw_zaful_recommend.zaful_app_category_xxx_cart_num_tmp where  platform='ios') g 
on a.add_time=g.add_time and a.af_inner_mediasource=g.af_inner_mediasource and a.language=g.language and a.country_code=g.country_code
left join 
(select cart_uv, add_time,af_inner_mediasource,language,country_code from   dw_zaful_recommend.zaful_app_category_xxx_cart_uv_tmp where  platform='ios') h 
on a.add_time=h.add_time and a.af_inner_mediasource=h.af_inner_mediasource and a.language=h.language and a.country_code=h.country_code
left join 
(select order_sku_num, add_time,af_inner_mediasource,language,country_code from   dw_zaful_recommend.zaful_app_category_xxx_order_sku_num_tmp where  platform='ios') i 
on a.add_time=i.add_time and a.af_inner_mediasource=i.af_inner_mediasource and a.language=i.language and a.country_code=i.country_code
left join 
(select order_uv, add_time,af_inner_mediasource,language,country_code from   dw_zaful_recommend.zaful_app_category_xxx_order_uv_tmp where  platform='ios') j 
on a.add_time=j.add_time and a.af_inner_mediasource=j.af_inner_mediasource and a.language=j.language and a.country_code=j.country_code
left join 
(select gmv, add_time,af_inner_mediasource,language,country_code from   dw_zaful_recommend.zaful_app_category_xxx_gmv_tmp where  platform='ios') k 
on a.add_time=k.add_time and a.af_inner_mediasource=k.af_inner_mediasource and a.language=k.language and a.country_code=k.country_code
left join 
(select purchase_num, add_time,af_inner_mediasource,language,country_code from   dw_zaful_recommend.zaful_app_category_xxx_purchase_num_tmp where  platform='ios') l
on a.add_time=l.add_time and a.af_inner_mediasource=l.af_inner_mediasource and a.language=l.language and a.country_code=l.country_code
left join 
(select pay_uv, add_time,af_inner_mediasource,language,country_code from   dw_zaful_recommend.zaful_app_category_xxx_pay_uv_tmp where  platform='ios') m
on a.add_time=m.add_time and a.af_inner_mediasource=m.af_inner_mediasource and a.language=m.language and a.country_code=m.country_code
left join 
(select pay_amount, add_time,af_inner_mediasource,language,country_code from   dw_zaful_recommend.zaful_app_category_xxx_pay_amount_tmp where  platform='ios') n
on a.add_time=n.add_time and a.af_inner_mediasource=n.af_inner_mediasource and a.language=n.language and a.country_code=n.country_code
left join 
(select collect_uv, add_time,af_inner_mediasource,language,country_code from   dw_zaful_recommend.zaful_app_category_xxx_collect_uv_tmp where  platform='ios') o
on a.add_time=o.add_time and a.af_inner_mediasource=o.af_inner_mediasource and a.language=o.language and a.country_code=o.country_code
left join 
(select collect_num, add_time,af_inner_mediasource,language,country_code from   dw_zaful_recommend.zaful_app_category_xxx_collect_num_tmp where  platform='ios') p
on a.add_time=p.add_time and a.af_inner_mediasource=p.af_inner_mediasource and a.language=p.language and a.country_code=p.country_code
;



--导出数据表
INSERT OVERWRITE TABLE dw_zaful_recommend.zaful_app_category_xxx_report_exp

select 
x.pv as pv ,
x.uv as uv,
x.exp_num as exp_num,
x.sku_uv as sku_uv,
x.click_num as click_num,
x.click_uv as click_uv,
x.exp_click_ratio as exp_click_ratio,
x.user_click_ratio as user_click_ratio,
x.cart_num as cart_num,
x.cart_uv as cart_uv,
x.sku_cart_ratio as sku_cart_ratio,
x.user_cart_ratio as user_cart_ratio,
x.order_sku_num as order_sku_num,
x.order_uv as order_uv,
x.sku_order_ratio as sku_order_ratio,
x.user_order_ratio as user_order_ratio,
x.gmv as gmv,
x.purchase_num as purchase_num,  
x.pay_uv as pay_uv,
x.pay_amount as pay_amount,
x.sku_purchase_ratio as sku_purchase_ratio,
x.user_purchase_ratio as user_purchase_ratio,
x.gmv_cost_mille as gmv_cost_mille,
x.collect_uv as collect_uv,
x.collect_num as collect_num,
x.platform as platform,
x.recommend_position as recommend_position,
x.position_name as position_name,
x.language as language,
y.country as country,
x.add_time  as add_time
from 
(
select
  *
from 
  dw_zaful_recommend.zaful_app_category_xxx_report
where 
  add_time = '${ADD_TIME}'
) x 
left join dw_zaful_recommend.cuntryode y on x.country_code=y.code


union all

select 
m.pv,
m.uv,
m.exp_num,
m.sku_uv,
m.click_num,
m.click_uv,
m.exp_click_ratio,
m.user_click_ratio,
m.cart_num,
m.cart_uv,
m.sku_cart_ratio,
m.user_cart_ratio,
m.order_sku_num,
m.order_uv,
m.sku_order_ratio,
m.user_order_ratio,
m.gmv,
m.purchase_num,  
m.pay_uv,
m.pay_amount,
m.sku_purchase_ratio,
m.user_purchase_ratio,
m.gmv_cost_mille,
m.collect_uv,
m.collect_num,
m.platform as platform,
'All' as recommend_position,
'列表页推荐' as position_name,
'all' as language,
'all' as country,
m.add_time as add_time
from 
(
select
  -- sum(pv) as pv,
  -- sum(uv) as uv,
  -- sum(exp_num) as exp_num,
  -- sum(sku_uv) as sku_uv,
  -- sum(click_num) as click_num,
  -- sum(click_uv) as click_uv,
  -- sum(click_num) / sum(exp_num) * 100 as exp_click_ratio,
  -- sum(click_uv) / sum(sku_uv) * 100 as user_click_ratio,
  -- sum(cart_num) as cart_num,
  -- sum(cart_uv) as cart_uv,
  -- sum(cart_num) / sum(exp_num) * 100 as sku_cart_ratio,
  -- sum(cart_uv) / sum(sku_uv) * 100 as user_cart_ratio,
  -- sum(order_sku_num) as order_sku_num,
  -- sum(order_uv) as order_uv,
  -- sum(order_sku_num) / sum(exp_num) * 100 as sku_order_ratio,
  -- sum(order_uv) / sum(sku_uv) * 100 as user_order_ratio,
  -- sum(gmv) as gmv,
  -- sum(purchase_num) as purchase_num,
  -- sum(pay_uv) as pay_uv,
  -- sum(pay_amount) as pay_amount,
  -- sum(purchase_num) / sum(exp_num) * 100 as sku_purchase_ratio,
  -- sum(pay_uv) / sum(sku_uv) * 100 as user_purchase_ratio,
  -- sum(gmv) / sum(exp_num) * 1000 as gmv_cost_mille,
  -- sum(collect_uv) as collect_uv,
  -- sum(collect_num) as collect_num,
  -- platform,
  -- '${ADD_TIME}' as add_time
 pv,
 uv,
 exp_num,
 sku_uv,
 click_num,
 click_uv,
 exp_click_ratio,
 user_click_ratio,
 cart_num,
 cart_uv,
 sku_cart_ratio,
 user_cart_ratio,
 order_sku_num,
 order_uv,
 sku_order_ratio,
 user_order_ratio,
 gmv,
 purchase_num,  
 pay_uv,
 pay_amount,
 sku_purchase_ratio,
 user_purchase_ratio,
 gmv_cost_mille,
 collect_uv,
 collect_num,
 platform as platform,
'All' as recommend_position,
'列表页推荐' as position_name,
'all' as language,
'all' as country,
 '${ADD_TIME}' as add_time

from 
  dw_zaful_recommend.zaful_app_category_xxx_all_report
where 
  add_time = '${ADD_TIME}'
) m 


;

--SELECT * FROM dw_zaful_recommend.cuntryode;











































































































































