--@author wuchao
--@date 2018年10月24日 
--@desc  Zaful App推荐位报表AB测试所有指标


SET mapred.job.name=rewrite_zaful_app_recommend_report;
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
CREATE TABLE IF NOT EXISTS dw_zaful_recommend.rewrite_zaful_app_abtest_allindex_report_gmv_export(
exp_num                   INT            COMMENT "商品曝光数",
sku_uv                    INT            COMMENT "查看商品UV",
click_num                 INT            COMMENT "商品点击数",
click_uv                  INT            COMMENT "点击UV",
exp_click_ratio           decimal(10,4)  COMMENT "曝光点击率",
user_click_ratio          decimal(10,4)  COMMENT "用户点击率",
cart_num                  INT            COMMENT "商品加购数",
cart_uv                   INT            COMMENT "加购UV",
sku_cart_ratio            decimal(10,6)  COMMENT "商品加购率",
user_cart_ratio           decimal(10,4)  COMMENT "用户加购率",
order_sku_num             INT            COMMENT "下单商品数",
order_uv                  INT            COMMENT "下单UV",
sku_order_ratio           decimal(10,6)  COMMENT "商品下单率",
user_order_ratio          decimal(10,5)  COMMENT "用户下单率",
purchase_num              INT            COMMENT "销量",  
pay_uv                    INT            COMMENT "付款uv",
pay_amount                INT            COMMENT "销售额",
sku_purchase_ratio        decimal(10,7)  COMMENT "商品购买转化率",
user_purchase_ratio       decimal(10,5)  COMMENT "用户购买转化率",
collect_uv                INT            COMMENT "商品收藏UV",
collect_num               INT            COMMENT "商品收藏数",
gmv_amount                INT            COMMENT "GMV",
gmv_order_ratio           decimal(10,6)  COMMENT "客单价",
gmv_exp_ratio             decimal(10,6)  COMMENT "成交转化率",
platform                  STRING         COMMENT "平台",
af_inner_mediasource      STRING         COMMENT "推荐位",
planid                    STRING         COMMENT "",
versionid                 STRING         comment "",
add_times                 STRING         comment "时间",
all_order_uv              INT            COMMENT "订单uv",
all_order_amount          INT            COMMENT "订单金额",
all_pay_uv                INT            COMMENT "付款uv",
all_pay_amount            INT            COMMENT "付款金额"
)
COMMENT 'ZAFUL APP AB测试数据报表allindex加gmv并拼接整体指标'
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\u0001' LINES TERMINATED BY '\n' STORED AS TEXTFILE
;

--输出结果表中间表
CREATE TABLE IF NOT EXISTS dw_zaful_recommend.rewrite_zaful_app_abtest_allindex_report_gmv(
exp_num                   INT            COMMENT "商品曝光数",
sku_uv                    INT            COMMENT "查看商品UV",
click_num                 INT            COMMENT "商品点击数",
click_uv                  INT            COMMENT "点击UV",
exp_click_ratio           decimal(10,4)  COMMENT "曝光点击率",
user_click_ratio          decimal(10,4)  COMMENT "用户点击率",
cart_num                  INT            COMMENT "商品加购数",
cart_uv                   INT            COMMENT "加购UV",
sku_cart_ratio            decimal(10,6)  COMMENT "商品加购率",
user_cart_ratio           decimal(10,4)  COMMENT "用户加购率",
order_sku_num             INT            COMMENT "下单商品数",
order_uv                  INT            COMMENT "下单UV",
sku_order_ratio           decimal(10,6)  COMMENT "商品下单率",
user_order_ratio          decimal(10,5)  COMMENT "用户下单率",
purchase_num              INT            COMMENT "销量",  
pay_uv                    INT            COMMENT "付款uv",
pay_amount                INT            COMMENT "销售额",
sku_purchase_ratio        decimal(10,7)  COMMENT "商品购买转化率",
user_purchase_ratio       decimal(10,5)  COMMENT "用户购买转化率",
collect_uv                INT            COMMENT "商品收藏UV",
collect_num               INT            COMMENT "商品收藏数",
gmv_amount                INT            COMMENT "GMV",
gmv_order_ratio           decimal(10,6)  COMMENT "客单价",
gmv_exp_ratio             decimal(10,6)  COMMENT "成交转化率",
platform                  STRING         COMMENT "平台",
af_inner_mediasource      STRING         COMMENT "推荐位",
planid                    STRING         COMMENT "",
versionid                 STRING         comment ""
)
COMMENT 'ZAFUL APP AB测试数据报表allindex加gmv并拼接整体指标'
PARTITIONED BY (add_time STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\u0001' LINES TERMINATED BY '\n' STORED AS TEXTFILE
;








--商品曝光数	
CREATE TABLE IF NOT EXISTS dw_zaful_recommend.rewrite_zaful_app_abtest_allindex_exp_num_tmp_gmv(
exp_num                   INT            COMMENT "商品曝光数",
platform                  STRING         COMMENT "平台",
af_inner_mediasource      STRING         COMMENT "推荐位",
planid                    STRING         COMMENT "",
versionid                 STRING         comment "",
add_time                  STRING         COMMENT "日期"
)
COMMENT "推荐位报表商品曝光数中间表"
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\u0001' LINES TERMINATED BY '\n' STORED AS TEXTFILE;


--查看商品UV	
CREATE TABLE IF NOT EXISTS dw_zaful_recommend.rewrite_zaful_app_abtest_allindex_sku_uv_tmp_gmv(
sku_uv                    INT            COMMENT "查看商品UV",
platform                  STRING         COMMENT "平台",
af_inner_mediasource      STRING         COMMENT "推荐位",
planid                    STRING         COMMENT "",
versionid                 STRING         comment "",
add_time                  STRING         COMMENT "日期"
)
COMMENT "推荐位报表查看商品UV中间表"
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\u0001' LINES TERMINATED BY '\n' STORED AS TEXTFILE;


--商品点击数	
CREATE TABLE IF NOT EXISTS dw_zaful_recommend.rewrite_zaful_app_abtest_allindex_click_num_tmp_gmv(
click_num                 INT            COMMENT "商品点击数",
platform                  STRING         COMMENT "平台",
af_inner_mediasource      STRING         COMMENT "推荐位",
planid                    STRING         COMMENT "",
versionid                 STRING         comment "",
add_time                  STRING         COMMENT "日期"
)
COMMENT "推荐位报表商品点击数中间表"
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\u0001' LINES TERMINATED BY '\n' STORED AS TEXTFILE;


--点击UV	
CREATE TABLE IF NOT EXISTS dw_zaful_recommend.rewrite_zaful_app_abtest_allindex_click_uv_tmp_gmv(
click_uv                  INT            COMMENT "点击UV",
platform                  STRING         COMMENT "平台",
af_inner_mediasource      STRING         COMMENT "推荐位",
planid                    STRING         COMMENT "",
versionid                 STRING         comment "",
add_time                  STRING         COMMENT "日期"
)
COMMENT "推荐位报表商品点击UV中间表"
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\u0001' LINES TERMINATED BY '\n' STORED AS TEXTFILE;


--商品加购数	
CREATE TABLE IF NOT EXISTS dw_zaful_recommend.rewrite_zaful_app_abtest_allindex_cart_num_tmp_gmv(
cart_num                  INT            COMMENT "商品加购数",
platform                  STRING         COMMENT "平台",
af_inner_mediasource      STRING         COMMENT "推荐位",
planid                    STRING         COMMENT "",
versionid                 STRING         comment "",
add_time                  STRING         COMMENT "日期"
)
COMMENT "推荐位报表商品加购数中间表"
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\u0001' LINES TERMINATED BY '\n' STORED AS TEXTFILE;


--加购UV	
CREATE TABLE IF NOT EXISTS dw_zaful_recommend.rewrite_zaful_app_abtest_allindex_cart_uv_tmp_gmv(
cart_uv                   INT            COMMENT "加购UV",
platform                  STRING         COMMENT "平台",
af_inner_mediasource      STRING         COMMENT "推荐位",
planid                    STRING         COMMENT "",
versionid                 STRING         comment "",
add_time                  STRING         COMMENT "日期"
)
COMMENT "推荐位报表商品加购UV中间表"
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\u0001' LINES TERMINATED BY '\n' STORED AS TEXTFILE;


--下单商品数	

--中间表rewrite_report_sku_user_tmp：取sku,user_id
CREATE TABLE IF NOT EXISTS dw_zaful_recommend.rewrite_zaful_app_abtest_allindex_sku_user_country_tmp_gmv(
goods_sn                  STRING         COMMENT "sku",
user_id                   STRING         COMMENT "用户ID",
platform                  STRING         COMMENT "平台",
af_inner_mediasource      STRING         COMMENT "推荐位",
planid                    STRING         COMMENT "",
versionid                 STRING         comment "",
add_time                  STRING         COMMENT "日期"
)
COMMENT "推荐位报表user-sku-county中间表"
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\u0001' LINES TERMINATED BY '\n' STORED AS TEXTFILE;


--中间表rewrite-uv_report_sku_user_tmp：取sku,user_id
CREATE TABLE IF NOT EXISTS dw_zaful_recommend.uv_rewrite_zaful_app_abtest_allindex_sku_user_country_tmp_gmv(
goods_sn                  STRING         COMMENT "sku",
user_id                   STRING         COMMENT "用户ID",
appsflyer_device_id       STRING         COMMENT "用户uv",
platform                  STRING         COMMENT "平台",
af_inner_mediasource      STRING         COMMENT "推荐位",
planid                    STRING         COMMENT "",
versionid                 STRING         comment "",
add_time                  STRING         COMMENT "日期"
)
COMMENT "推荐位报表user-sku-county中间表"
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\u0001' LINES TERMINATED BY '\n' STORED AS TEXTFILE;



CREATE TABLE IF NOT EXISTS dw_zaful_recommend.rewrite_zaful_app_abtest_allindex_sku_user_tmp_gmv(
goods_sn                  STRING         COMMENT "sku",
user_id                   STRING         COMMENT "用户ID",
order_status              STRING         COMMENT "订单状态",
pay_amount                decimal(10,2)  COMMENT "购买金额",
goods_number              STRING         COMMENT "商品数量",
order_id                  STRING         COMMENT "订单id"
)
COMMENT "推荐位报表user-sku中间表"
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\u0001' LINES TERMINATED BY '\n' STORED AS TEXTFILE;


--下单商品数结果表
CREATE TABLE IF NOT EXISTS dw_zaful_recommend.rewrite_zaful_app_abtest_allindex_order_sku_num_tmp_gmv(
order_sku_num                 INT        COMMENT "下单商品数",
platform                  STRING         COMMENT "平台",
af_inner_mediasource      STRING         COMMENT "推荐位",
planid                    STRING         COMMENT "",
versionid                 STRING         comment "",
add_time                  STRING         COMMENT "日期"
)
COMMENT "推荐位报表下单商品数中间表"
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\u0001' LINES TERMINATED BY '\n' STORED AS TEXTFILE;


--下单uv
CREATE TABLE IF NOT EXISTS dw_zaful_recommend.rewrite_zaful_app_abtest_allindex_order_uv_tmp_gmv(
order_uv                  INT            COMMENT "下单UV",
platform                  STRING         COMMENT "平台",
af_inner_mediasource      STRING         COMMENT "推荐位",
planid                    STRING         COMMENT "",
versionid                 STRING         comment "",
add_time                  STRING         COMMENT "日期"
)
COMMENT "推荐位报表下单uv中间表"
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\u0001' LINES TERMINATED BY '\n' STORED AS TEXTFILE;


--销量
CREATE TABLE IF NOT EXISTS dw_zaful_recommend.rewrite_zaful_app_abtest_allindex_purchase_num_tmp_gmv(
purchase_num              INT            COMMENT "销量",
platform                  STRING         COMMENT "平台",
af_inner_mediasource      STRING         COMMENT "推荐位",
planid                    STRING         COMMENT "",
versionid                 STRING         comment "",
add_time                  STRING         COMMENT "日期"
)
COMMENT "推荐位报表销量中间表"
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\u0001' LINES TERMINATED BY '\n' STORED AS TEXTFILE;



--付款uv
CREATE TABLE IF NOT EXISTS dw_zaful_recommend.rewrite_zaful_app_abtest_allindex_pay_uv_tmp_gmv(
pay_uv                    INT            COMMENT "付款uv",
platform                  STRING         COMMENT "平台",
af_inner_mediasource      STRING         COMMENT "推荐位",
planid                    STRING         COMMENT "",
versionid                 STRING         comment "",
add_time                  STRING         COMMENT "日期"
)
COMMENT "推荐位报表付款uv中间表"
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\u0001' LINES TERMINATED BY '\n' STORED AS TEXTFILE;


--销售额
CREATE TABLE IF NOT EXISTS dw_zaful_recommend.rewrite_zaful_app_abtest_allindex_pay_amount_tmp_gmv(
pay_amount                decimal(10,2)  COMMENT "购买金额",
platform                  STRING         COMMENT "平台",
af_inner_mediasource      STRING         COMMENT "推荐位",
planid                    STRING         COMMENT "",
versionid                 STRING         comment "",
add_time                  STRING         COMMENT "日期"
)
COMMENT "推荐位报表购买金额中间表"
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\u0001' LINES TERMINATED BY '\n' STORED AS TEXTFILE;


--商品收藏UV	
CREATE TABLE IF NOT EXISTS dw_zaful_recommend.rewrite_zaful_app_abtest_allindex_collect_uv_tmp_gmv(
collect_uv                INT            COMMENT "商品收藏UV",
platform                  STRING         COMMENT "平台",
af_inner_mediasource      STRING         COMMENT "推荐位",
planid                    STRING         COMMENT "",
versionid                 STRING         comment "",
add_time                  STRING         COMMENT "日期"
)
COMMENT "推荐位报表商品收藏UV中间表"
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\u0001' LINES TERMINATED BY '\n' STORED AS TEXTFILE;

--商品收藏数	
CREATE TABLE IF NOT EXISTS dw_zaful_recommend.rewrite_zaful_app_abtest_allindex_collect_num_tmp_gmv(
collect_num               INT            COMMENT "商品收藏数",
platform                  STRING         COMMENT "平台",
af_inner_mediasource      STRING         COMMENT "推荐位",
planid                    STRING         COMMENT "",
versionid                 STRING         comment "",
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
INSERT OVERWRITE TABLE dw_zaful_recommend.rewrite_zaful_app_abtest_allindex_sku_user_country_tmp_gmv
SELECT
  m.goods_sn,
  n.customer_user_id,
  m.platform,
  m.af_inner_mediasource,
  m.planid,
  m.versionid,
  '${ADD_TIME}' as add_time
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


--中间表report_uv_sku_user_tmp：取sku,user_id
INSERT OVERWRITE TABLE dw_zaful_recommend.uv_rewrite_zaful_app_abtest_allindex_sku_user_country_tmp_gmv
SELECT
  m.goods_sn,
  n.customer_user_id,
  m.appsflyer_device_id,
  m.platform,
  m.af_inner_mediasource,
  m.planid,
  m.versionid,
  '${ADD_TIME}' as add_time
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
  m.appsflyer_device_id,
  m.goods_sn,
  n.customer_user_id,
  m.platform,
  m.af_inner_mediasource,
  m.planid,
  m.versionid
;


--中间表report_sku_user_tmp：取sku,user_id
INSERT OVERWRITE TABLE dw_zaful_recommend.rewrite_zaful_app_abtest_allindex_sku_user_tmp_gmv
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





--商品曝光数
INSERT OVERWRITE TABLE dw_zaful_recommend.rewrite_zaful_app_abtest_allindex_exp_num_tmp_gmv
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
  get_json_object(event_value, '$.af_plan_id') as planid,
	 get_json_object(event_value, '$.af_version_id') as versionid
  
  FROM
  ods.ods_app_burial_log 
  WHERE
  concat_ws('-', year, month, day) = '${ADD_TIME}'
  and event_name='af_impression'
  and get_json_object(event_value, '$.af_inner_mediasource') in ('recommend_homepage','recommend productdetail')
      and site='zaful'
) m 
group by
m.platform,
m.af_inner_mediasource,
m.planid,
m.versionid
;


--查看商品UV
INSERT OVERWRITE TABLE dw_zaful_recommend.rewrite_zaful_app_abtest_allindex_sku_uv_tmp_gmv
SELECT
  count(*) AS sku_uv,
  m.platform,
  m.af_inner_mediasource,
  m.planid,
  m.versionid,
  '${ADD_TIME}' as add_time
FROM
  (
    SELECT
      appsflyer_device_id,
      platform,
      'recommend_homepage' as af_inner_mediasource,
      get_json_object(event_value, '$.af_plan_id') as planid,
	    get_json_object(event_value, '$.af_version_id') as versionid
    FROM
      ods.ods_app_burial_log 
    WHERE
      concat_ws('-', year, month, day) = '${ADD_TIME}'
        and event_name='af_impression'
		and get_json_object(event_value, '$.af_inner_mediasource') in ('recommend_homepage')
        and site='zaful'
    GROUP BY
      appsflyer_device_id,
      platform,
      get_json_object(event_value, '$.af_plan_id'),
      get_json_object(event_value, '$.af_version_id')
	union all
	SELECT
      appsflyer_device_id,
      platform,
      'recommend productdetail' as af_inner_mediasource,
       get_json_object(event_value, '$.af_plan_id') as planid,
	     get_json_object(event_value, '$.af_version_id') as versionid
    FROM
      ods.ods_app_burial_log 
    WHERE
      concat_ws('-', year, month, day) = '${ADD_TIME}'
        and event_name='af_impression'
		and get_json_object(event_value, '$.af_inner_mediasource') in ('recommend productdetail')
        and site='zaful'
    GROUP BY
      appsflyer_device_id,
      platform,
      get_json_object(event_value, '$.af_plan_id'),
      get_json_object(event_value, '$.af_version_id')
  ) m
group by
  m.platform,
  m.af_inner_mediasource,
  m.planid,
  m.versionid
  ;


--商品点击数
INSERT OVERWRITE TABLE dw_zaful_recommend.rewrite_zaful_app_abtest_allindex_click_num_tmp_gmv
SELECT
  count(*) AS click_num,
  platform,
  get_json_object(event_value, '$.af_inner_mediasource') as af_inner_mediasource,
  get_json_object(event_value, '$.af_plan_id') as planid,
	 get_json_object(event_value, '$.af_version_id') as versionid,
  '${ADD_TIME}' as add_time
FROM
  ods.ods_app_burial_log 
WHERE
  concat_ws('-', year, month, day) = '${ADD_TIME}'
  AND event_name='af_view_product'
  AND get_json_object(event_value, '$.af_changed_size_or_color') = 0
  and get_json_object(event_value, '$.af_inner_mediasource') in ('recommend_homepage','recommend productdetail')
      and site='zaful'
group by
  platform,
  get_json_object(event_value, '$.af_inner_mediasource'),
  get_json_object(event_value, '$.af_plan_id'),
	get_json_object(event_value, '$.af_version_id')
;


--点击UV
INSERT OVERWRITE TABLE dw_zaful_recommend.rewrite_zaful_app_abtest_allindex_click_uv_tmp_gmv
SELECT
  count(*) AS click_uv,
  m.platform,
  m.af_inner_mediasource,
  m.planid,
  m.versionid,
  '${ADD_TIME}' as add_time
FROM
  (
    SELECT
      appsflyer_device_id,
      platform,
      'recommend_homepage' as af_inner_mediasource,
      get_json_object(event_value, '$.af_plan_id') as planid,
	    get_json_object(event_value, '$.af_version_id') as versionid
    FROM
      ods.ods_app_burial_log 
    WHERE
      concat_ws('-', year, month, day) = '${ADD_TIME}'
        and event_name='af_view_product'
		AND get_json_object(event_value, '$.af_changed_size_or_color') = 0
		and get_json_object(event_value, '$.af_inner_mediasource') in ('recommend_homepage')
        and site='zaful'
    GROUP BY
      appsflyer_device_id,
      platform,
      get_json_object(event_value, '$.af_plan_id'),
	    get_json_object(event_value, '$.af_version_id')
	union all
	SELECT
      appsflyer_device_id,
      platform,
      'recommend productdetail' as af_inner_mediasource,
      get_json_object(event_value, '$.af_plan_id') as planid,
	    get_json_object(event_value, '$.af_version_id') as versionid
    FROM
      ods.ods_app_burial_log 
    WHERE
      concat_ws('-', year, month, day) = '${ADD_TIME}'
        and event_name='af_view_product'
		AND get_json_object(event_value, '$.af_changed_size_or_color') = 0
		and get_json_object(event_value, '$.af_inner_mediasource') in ('recommend productdetail')
        and site='zaful'
    GROUP BY
      appsflyer_device_id,
      platform,
      get_json_object(event_value, '$.af_plan_id'),
	    get_json_object(event_value, '$.af_version_id')
  ) m
group by
  m.platform,
  m.af_inner_mediasource,
  m.planid,
  m.versionid
  ;

  
  
--商品加购数
INSERT OVERWRITE TABLE dw_zaful_recommend.rewrite_zaful_app_abtest_allindex_cart_num_tmp_gmv
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
  get_json_object(event_value, '$.af_plan_id') as planid,
	get_json_object(event_value, '$.af_version_id') as versionid
  FROM
  ods.ods_app_burial_log 
  WHERE
  concat_ws('-', year, month, day) = '${ADD_TIME}'
  and event_name='af_add_to_bag'
  and get_json_object(event_value, '$.af_inner_mediasource') in ('recommend_homepage','recommend productdetail')
      and site='zaful'
) m 
group by
m.platform,
m.af_inner_mediasource,
m.planid,
m.versionid
;

--加购UV
INSERT OVERWRITE TABLE dw_zaful_recommend.rewrite_zaful_app_abtest_allindex_cart_uv_tmp_gmv
SELECT
  count(*) AS cart_uv,
  m.platform,
  m.af_inner_mediasource,
  m.planid,
  m.versionid,
  '${ADD_TIME}' as add_time
FROM
  (
    SELECT
      appsflyer_device_id,
      platform,
      'recommend_homepage' as af_inner_mediasource,
      get_json_object(event_value, '$.af_plan_id') as planid,
	    get_json_object(event_value, '$.af_version_id') as versionid
    FROM
      ods.ods_app_burial_log 
    WHERE
      concat_ws('-', year, month, day) = '${ADD_TIME}'
        and event_name='af_add_to_bag'
		and get_json_object(event_value, '$.af_inner_mediasource') in ('recommend_homepage')
        and site='zaful'
    GROUP BY
      appsflyer_device_id,
      platform,
      get_json_object(event_value, '$.af_plan_id'),
	    get_json_object(event_value, '$.af_version_id')
	union all
	SELECT
      appsflyer_device_id,
      platform,
      'recommend productdetail' as af_inner_mediasource,
      get_json_object(event_value, '$.af_plan_id') as planid,
	    get_json_object(event_value, '$.af_version_id') as versionid
    FROM
      ods.ods_app_burial_log 
    WHERE
      concat_ws('-', year, month, day) = '${ADD_TIME}'
        and event_name='af_add_to_bag'
		and get_json_object(event_value, '$.af_inner_mediasource') in ('recommend productdetail')
        and site='zaful'
    GROUP BY
      appsflyer_device_id,
      platform,
      get_json_object(event_value, '$.af_plan_id'),
	    get_json_object(event_value, '$.af_version_id')
  ) m
group by
  m.platform,
  m.af_inner_mediasource,
  m.planid,
  m.versionid
  ;


--下单商品数
INSERT OVERWRITE TABLE dw_zaful_recommend.rewrite_zaful_app_abtest_allindex_order_sku_num_tmp_gmv
SELECT
  SUM(x2.goods_number) AS order_num,
  x1.platform,
  x1.af_inner_mediasource,
  x1.planid,
  x1.versionid,
  '${ADD_TIME}' as add_time
from
  dw_zaful_recommend.rewrite_zaful_app_abtest_allindex_sku_user_country_tmp x1
  INNER JOIN dw_zaful_recommend.rewrite_zaful_app_abtest_allindex_sku_user_tmp x2 ON x1.user_id = x2.user_id
  AND x1.goods_sn = x2.goods_sn
group by
  x1.platform,
  x1.af_inner_mediasource,
  x1.planid,
  x1.versionid
;


--下单UV
INSERT OVERWRITE TABLE dw_zaful_recommend.rewrite_zaful_app_abtest_allindex_order_uv_tmp_gmv
SELECT
  count(distinct x1.appsflyer_device_id) as order_uv,
  x1.platform,
  x1.af_inner_mediasource,
  x1.planid,
  x1.versionid,
  '${ADD_TIME}' as add_time
from
  dw_zaful_recommend.uv_rewrite_zaful_app_abtest_allindex_sku_user_country_tmp x1
  INNER JOIN dw_zaful_recommend.rewrite_zaful_app_abtest_allindex_sku_user_tmp x2 ON x1.user_id = x2.user_id
  AND x1.goods_sn = x2.goods_sn
group by
  x1.platform,
  x1.af_inner_mediasource,
  x1.planid,
  x1.versionid
;

--销量
INSERT OVERWRITE TABLE dw_zaful_recommend.rewrite_zaful_app_abtest_allindex_purchase_num_tmp_gmv
SELECT
  SUM(x2.goods_number) AS purchase_num,
  x1.platform,
  x1.af_inner_mediasource,
  x1.planid,
  x1.versionid,
  '${ADD_TIME}' as add_time
from
  dw_zaful_recommend.rewrite_zaful_app_abtest_allindex_sku_user_country_tmp x1
  INNER JOIN dw_zaful_recommend.rewrite_zaful_app_abtest_allindex_sku_user_tmp x2 ON x1.user_id = x2.user_id
  AND x1.goods_sn = x2.goods_sn
where
  x2.order_status not in ('0', '11')
group by
  x1.platform,
  x1.af_inner_mediasource,
  x1.planid,
  x1.versionid
;


--付款UV
INSERT OVERWRITE TABLE dw_zaful_recommend.rewrite_zaful_app_abtest_allindex_pay_uv_tmp_gmv
SELECT
  count(distinct x1.appsflyer_device_id) as pay_uv,
  x1.platform,
  x1.af_inner_mediasource,
  x1.planid,
  x1.versionid,
  '${ADD_TIME}' as add_time
from
  dw_zaful_recommend.uv_rewrite_zaful_app_abtest_allindex_sku_user_country_tmp x1
  INNER JOIN dw_zaful_recommend.rewrite_zaful_app_abtest_allindex_sku_user_tmp x2 ON x1.user_id = x2.user_id
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
INSERT OVERWRITE TABLE dw_zaful_recommend.rewrite_zaful_app_abtest_allindex_pay_amount_tmp_gmv
SELECT
  sum(x2.pay_amount) as pay_amount,
  x1.platform,
  x1.af_inner_mediasource,
  x1.planid,
  x1.versionid,
  '${ADD_TIME}' as add_time
from
  dw_zaful_recommend.rewrite_zaful_app_abtest_allindex_sku_user_country_tmp x1
  INNER JOIN dw_zaful_recommend.rewrite_zaful_app_abtest_allindex_sku_user_tmp x2 ON x1.user_id = x2.user_id
  AND x1.goods_sn = x2.goods_sn
where
  x2.order_status not in ('0', '11')
group by
  x1.platform,
  x1.af_inner_mediasource,
  x1.planid,
  x1.versionid
  ;


--商品收藏UV	
INSERT OVERWRITE TABLE dw_zaful_recommend.rewrite_zaful_app_abtest_allindex_collect_uv_tmp_gmv
SELECT
  count(distinct appsflyer_device_id) as collect_uv,
  platform,
  get_json_object(event_value, '$.af_inner_mediasource') AS af_inner_mediasource,
  get_json_object(event_value, '$.af_plan_id') as planid,
	get_json_object(event_value, '$.af_version_id') as versionid,
  '${ADD_TIME}' as add_time
   FROM
  ods.ods_app_burial_log 
  WHERE
  concat_ws('-', year, month, day) = '${ADD_TIME}'
  and event_name='af_add_to_wishlist'
  and get_json_object(event_value, '$.af_inner_mediasource') in ('recommend_homepage','recommend productdetail')
      and site='zaful'
group by
  platform,
  get_json_object(event_value, '$.af_inner_mediasource'),
  get_json_object(event_value, '$.af_plan_id'),
	get_json_object(event_value, '$.af_version_id')
  ;
  

--商品收藏数	
INSERT OVERWRITE TABLE dw_zaful_recommend.rewrite_zaful_app_abtest_allindex_collect_num_tmp_gmv
SELECT
  count(*) as collect_num,
  platform,
  get_json_object(event_value, '$.af_inner_mediasource') AS af_inner_mediasource,
  get_json_object(event_value, '$.af_plan_id') as planid,
	get_json_object(event_value, '$.af_version_id') as versionid,
  '${ADD_TIME}' as add_time
   FROM
  ods.ods_app_burial_log 
  WHERE
  concat_ws('-', year, month, day) = '${ADD_TIME}'
  and event_name='af_add_to_wishlist'
  and get_json_object(event_value, '$.af_inner_mediasource') in ('recommend_homepage','recommend productdetail')
      and site='zaful'
group by
  platform,
  get_json_object(event_value, '$.af_inner_mediasource'),
  get_json_object(event_value, '$.af_plan_id'),
	get_json_object(event_value, '$.af_version_id')
  ;


--GMV
CREATE TABLE IF NOT EXISTS dw_zaful_recommend.rewrite_zaful_app_abtest_allindex_gmv_amount_tmp_gmv(
gmv_amount                decimal(10,2)  COMMENT "gmv",
platform                  STRING         COMMENT "平台",
af_inner_mediasource      STRING         COMMENT "推荐位",
planid                    STRING         COMMENT "",
versionid                 STRING         comment "",
add_time                  STRING         COMMENT "日期"
)
COMMENT "推荐位报表购买金额中间表"
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\u0001' LINES TERMINATED BY '\n' STORED AS TEXTFILE;

INSERT OVERWRITE TABLE dw_zaful_recommend.rewrite_zaful_app_abtest_allindex_gmv_amount_tmp_gmv
SELECT
  sum(x2.pay_amount) as gmv_amount,
  x1.platform,
  x1.af_inner_mediasource,
  x1.planid,
  x1.versionid,
  '${ADD_TIME}' as add_time
from
  dw_zaful_recommend.rewrite_zaful_app_abtest_allindex_sku_user_country_tmp x1
  INNER JOIN dw_zaful_recommend.rewrite_zaful_app_abtest_allindex_sku_user_tmp x2 ON x1.user_id = x2.user_id
  AND x1.goods_sn = x2.goods_sn
group by
  x1.platform,
  x1.af_inner_mediasource,
  x1.planid,
  x1.versionid
  ;


--所有结果汇总
INSERT overwrite TABLE dw_zaful_recommend.rewrite_zaful_app_abtest_allindex_report_gmv PARTITION (add_time = '${ADD_TIME}')
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
  i.order_sku_num / c.exp_num,
  j.order_uv / d.sku_uv,
  k.purchase_num,
  l.pay_uv,
  m.pay_amount,
  k.purchase_num / c.exp_num,
  l.pay_uv / d.sku_uv,
  n.collect_uv,
  o.collect_num,
  p.gmv_amount,
  p.gmv_amount / j.order_uv,
  p.gmv_amount / c.exp_num,
  'android',
  'recommend productdetail',
  c.planid,
  c.versionid
from   
(select exp_num, planid,versionid,add_time from dw_zaful_recommend.rewrite_zaful_app_abtest_allindex_exp_num_tmp_gmv where af_inner_mediasource='recommend productdetail' and platform='android') c 
left join 
(select sku_uv, planid,versionid,add_time from dw_zaful_recommend.rewrite_zaful_app_abtest_allindex_sku_uv_tmp_gmv where af_inner_mediasource='recommend productdetail' and platform='android') d 
on c.add_time=d.add_time and c.planid=d.planid and c.versionid=d.versionid
left join 
(select click_num,planid,versionid, add_time from dw_zaful_recommend.rewrite_zaful_app_abtest_allindex_click_num_tmp_gmv where af_inner_mediasource='recommend productdetail' and platform='android') e 
on c.add_time=e.add_time and c.planid=e.planid and c.versionid=e.versionid
left join 
(select click_uv, planid,versionid,add_time from dw_zaful_recommend.rewrite_zaful_app_abtest_allindex_click_uv_tmp_gmv where af_inner_mediasource='recommend productdetail' and platform='android') f 
on c.add_time=f.add_time and c.planid=f.planid and c.versionid=f.versionid
left join 
(select cart_num,planid,versionid, add_time from dw_zaful_recommend.rewrite_zaful_app_abtest_allindex_cart_num_tmp_gmv where af_inner_mediasource='recommend productdetail' and platform='android') g 
on c.add_time=g.add_time and c.planid=g.planid and c.versionid=g.versionid
left join 
(select cart_uv,planid,versionid, add_time from dw_zaful_recommend.rewrite_zaful_app_abtest_allindex_cart_uv_tmp_gmv where af_inner_mediasource='recommend productdetail' and platform='android') h 
on c.add_time=h.add_time and c.planid=h.planid and c.versionid=h.versionid
left join 
(select order_sku_num, planid,versionid,add_time from dw_zaful_recommend.rewrite_zaful_app_abtest_allindex_order_sku_num_tmp_gmv where af_inner_mediasource='recommend productdetail' and platform='android') i 
on c.add_time=i.add_time and c.planid=i.planid and c.versionid=i.versionid
left join 
(select order_uv,planid,versionid, add_time from dw_zaful_recommend.rewrite_zaful_app_abtest_allindex_order_uv_tmp_gmv where af_inner_mediasource='recommend productdetail' and platform='android') j 
on c.add_time=j.add_time and c.planid=j.planid and c.versionid=j.versionid
left join 
(select purchase_num, planid,versionid,add_time from dw_zaful_recommend.rewrite_zaful_app_abtest_allindex_purchase_num_tmp_gmv where af_inner_mediasource='recommend productdetail' and platform='android') k 
on c.add_time=k.add_time and c.planid=k.planid and c.versionid=k.versionid
left join 
(select pay_uv, planid,versionid,add_time from dw_zaful_recommend.rewrite_zaful_app_abtest_allindex_pay_uv_tmp_gmv where af_inner_mediasource='recommend productdetail' and platform='android') l 
on c.add_time=l.add_time and c.planid=l.planid and c.versionid=l.versionid
left join 
(select pay_amount,planid,versionid, add_time from dw_zaful_recommend.rewrite_zaful_app_abtest_allindex_pay_amount_tmp_gmv where af_inner_mediasource='recommend productdetail' and platform='android') m 
on c.add_time=m.add_time and c.planid=m.planid and c.versionid=m.versionid
left join 
(select collect_uv, planid,versionid,add_time from dw_zaful_recommend.rewrite_zaful_app_abtest_allindex_collect_uv_tmp_gmv where af_inner_mediasource='recommend productdetail' and platform='android') n 
on c.add_time=n.add_time and c.planid=n.planid and c.versionid=n.versionid
left join 
(select collect_num, planid,versionid,add_time from dw_zaful_recommend.rewrite_zaful_app_abtest_allindex_collect_num_tmp_gmv where af_inner_mediasource='recommend productdetail' and platform='android') o 
on c.add_time=o.add_time and c.planid=o.planid and c.versionid=o.versionid
left join
(select gmv_amount, planid,versionid,add_time from dw_zaful_recommend.rewrite_zaful_app_abtest_allindex_gmv_amount_tmp_gmv where af_inner_mediasource='recommend productdetail' and platform='android') p 
on c.add_time=p.add_time and c.planid=p.planid and c.versionid=p.versionid
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
  i.order_sku_num / c.exp_num,
  j.order_uv / d.sku_uv,
  k.purchase_num,
  l.pay_uv,
  m.pay_amount,
  k.purchase_num / c.exp_num,
  l.pay_uv / d.sku_uv,
  n.collect_uv,
  o.collect_num,
  p.gmv_amount,
  p.gmv_amount / j.order_uv,
  p.gmv_amount / c.exp_num,
  'android',
  'recommend_homepage',
  c.planid,
  c.versionid
from   
(select exp_num, planid,versionid,add_time from dw_zaful_recommend.rewrite_zaful_app_abtest_allindex_exp_num_tmp_gmv where af_inner_mediasource='recommend_homepage' and platform='android') c 
left join 
(select sku_uv, planid,versionid,add_time from dw_zaful_recommend.rewrite_zaful_app_abtest_allindex_sku_uv_tmp_gmv where af_inner_mediasource='recommend_homepage' and platform='android') d 
on c.add_time=d.add_time and c.planid=d.planid and c.versionid=d.versionid
left join 
(select click_num,planid,versionid, add_time from dw_zaful_recommend.rewrite_zaful_app_abtest_allindex_click_num_tmp_gmv where af_inner_mediasource='recommend_homepage' and platform='android') e 
on c.add_time=e.add_time and c.planid=e.planid and c.versionid=e.versionid
left join 
(select click_uv, planid,versionid,add_time from dw_zaful_recommend.rewrite_zaful_app_abtest_allindex_click_uv_tmp_gmv where af_inner_mediasource='recommend_homepage' and platform='android') f 
on c.add_time=f.add_time and c.planid=f.planid and c.versionid=f.versionid
left join 
(select cart_num,planid,versionid, add_time from dw_zaful_recommend.rewrite_zaful_app_abtest_allindex_cart_num_tmp_gmv where af_inner_mediasource='recommend_homepage' and platform='android') g 
on c.add_time=g.add_time and c.planid=g.planid and c.versionid=g.versionid
left join 
(select cart_uv,planid,versionid, add_time from dw_zaful_recommend.rewrite_zaful_app_abtest_allindex_cart_uv_tmp_gmv where af_inner_mediasource='recommend_homepage' and platform='android') h 
on c.add_time=h.add_time and c.planid=h.planid and c.versionid=h.versionid
left join 
(select order_sku_num, planid,versionid,add_time from dw_zaful_recommend.rewrite_zaful_app_abtest_allindex_order_sku_num_tmp_gmv where af_inner_mediasource='recommend_homepage' and platform='android') i 
on c.add_time=i.add_time and c.planid=i.planid and c.versionid=i.versionid
left join 
(select order_uv,planid,versionid, add_time from dw_zaful_recommend.rewrite_zaful_app_abtest_allindex_order_uv_tmp_gmv where af_inner_mediasource='recommend_homepage' and platform='android') j 
on c.add_time=j.add_time and c.planid=j.planid and c.versionid=j.versionid
left join 
(select purchase_num, planid,versionid,add_time from dw_zaful_recommend.rewrite_zaful_app_abtest_allindex_purchase_num_tmp_gmv where af_inner_mediasource='recommend_homepage' and platform='android') k 
on c.add_time=k.add_time and c.planid=k.planid and c.versionid=k.versionid
left join 
(select pay_uv, planid,versionid,add_time from dw_zaful_recommend.rewrite_zaful_app_abtest_allindex_pay_uv_tmp_gmv where af_inner_mediasource='recommend_homepage' and platform='android') l 
on c.add_time=l.add_time and c.planid=l.planid and c.versionid=l.versionid
left join 
(select pay_amount,planid,versionid, add_time from dw_zaful_recommend.rewrite_zaful_app_abtest_allindex_pay_amount_tmp_gmv where af_inner_mediasource='recommend_homepage' and platform='android') m 
on c.add_time=m.add_time and c.planid=m.planid and c.versionid=m.versionid
left join 
(select collect_uv, planid,versionid,add_time from dw_zaful_recommend.rewrite_zaful_app_abtest_allindex_collect_uv_tmp_gmv where af_inner_mediasource='recommend_homepage' and platform='android') n 
on c.add_time=n.add_time and c.planid=n.planid and c.versionid=n.versionid
left join 
(select collect_num, planid,versionid,add_time from dw_zaful_recommend.rewrite_zaful_app_abtest_allindex_collect_num_tmp_gmv where af_inner_mediasource='recommend_homepage' and platform='android') o 
on c.add_time=o.add_time and c.planid=o.planid and c.versionid=o.versionid
left join
(select gmv_amount, planid,versionid,add_time from dw_zaful_recommend.rewrite_zaful_app_abtest_allindex_gmv_amount_tmp_gmv where af_inner_mediasource='recommend_homepage' and platform='android') p 
on c.add_time=p.add_time and c.planid=p.planid and c.versionid=p.versionid
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
  i.order_sku_num / c.exp_num,
  j.order_uv / d.sku_uv,
  k.purchase_num,
  l.pay_uv,
  m.pay_amount,
  k.purchase_num / c.exp_num,
  l.pay_uv / d.sku_uv,
  n.collect_uv,
  o.collect_num,
  p.gmv_amount,
  p.gmv_amount / j.order_uv,
  p.gmv_amount / c.exp_num,
  'ios',
  'recommend productdetail',
  c.planid,
  c.versionid
from   
(select exp_num, planid,versionid,add_time from dw_zaful_recommend.rewrite_zaful_app_abtest_allindex_exp_num_tmp_gmv where af_inner_mediasource='recommend productdetail' and platform='ios') c 
left join 
(select sku_uv, planid,versionid,add_time from dw_zaful_recommend.rewrite_zaful_app_abtest_allindex_sku_uv_tmp_gmv where af_inner_mediasource='recommend productdetail' and platform='ios') d 
on c.add_time=d.add_time and c.planid=d.planid and c.versionid=d.versionid
left join 
(select click_num,planid,versionid, add_time from dw_zaful_recommend.rewrite_zaful_app_abtest_allindex_click_num_tmp_gmv where af_inner_mediasource='recommend productdetail' and platform='ios') e 
on c.add_time=e.add_time and c.planid=e.planid and c.versionid=e.versionid
left join 
(select click_uv, planid,versionid,add_time from dw_zaful_recommend.rewrite_zaful_app_abtest_allindex_click_uv_tmp_gmv where af_inner_mediasource='recommend productdetail' and platform='ios') f 
on c.add_time=f.add_time and c.planid=f.planid and c.versionid=f.versionid
left join 
(select cart_num,planid,versionid, add_time from dw_zaful_recommend.rewrite_zaful_app_abtest_allindex_cart_num_tmp_gmv where af_inner_mediasource='recommend productdetail' and platform='ios') g 
on c.add_time=g.add_time and c.planid=g.planid and c.versionid=g.versionid
left join 
(select cart_uv,planid,versionid, add_time from dw_zaful_recommend.rewrite_zaful_app_abtest_allindex_cart_uv_tmp_gmv where af_inner_mediasource='recommend productdetail' and platform='ios') h 
on c.add_time=h.add_time and c.planid=h.planid and c.versionid=h.versionid
left join 
(select order_sku_num, planid,versionid,add_time from dw_zaful_recommend.rewrite_zaful_app_abtest_allindex_order_sku_num_tmp_gmv where af_inner_mediasource='recommend productdetail' and platform='ios') i 
on c.add_time=i.add_time and c.planid=i.planid and c.versionid=i.versionid
left join 
(select order_uv,planid,versionid, add_time from dw_zaful_recommend.rewrite_zaful_app_abtest_allindex_order_uv_tmp_gmv where af_inner_mediasource='recommend productdetail' and platform='ios') j 
on c.add_time=j.add_time and c.planid=j.planid and c.versionid=j.versionid
left join 
(select purchase_num, planid,versionid,add_time from dw_zaful_recommend.rewrite_zaful_app_abtest_allindex_purchase_num_tmp_gmv where af_inner_mediasource='recommend productdetail' and platform='ios') k 
on c.add_time=k.add_time and c.planid=k.planid and c.versionid=k.versionid
left join 
(select pay_uv, planid,versionid,add_time from dw_zaful_recommend.rewrite_zaful_app_abtest_allindex_pay_uv_tmp_gmv where af_inner_mediasource='recommend productdetail' and platform='ios') l 
on c.add_time=l.add_time and c.planid=l.planid and c.versionid=l.versionid
left join 
(select pay_amount,planid,versionid, add_time from dw_zaful_recommend.rewrite_zaful_app_abtest_allindex_pay_amount_tmp_gmv where af_inner_mediasource='recommend productdetail' and platform='ios') m 
on c.add_time=m.add_time and c.planid=m.planid and c.versionid=m.versionid
left join 
(select collect_uv, planid,versionid,add_time from dw_zaful_recommend.rewrite_zaful_app_abtest_allindex_collect_uv_tmp_gmv where af_inner_mediasource='recommend productdetail' and platform='ios') n 
on c.add_time=n.add_time and c.planid=n.planid and c.versionid=n.versionid
left join 
(select collect_num, planid,versionid,add_time from dw_zaful_recommend.rewrite_zaful_app_abtest_allindex_collect_num_tmp_gmv where af_inner_mediasource='recommend productdetail' and platform='ios') o 
on c.add_time=o.add_time and c.planid=o.planid and c.versionid=o.versionid
left join
(select gmv_amount, planid,versionid,add_time from dw_zaful_recommend.rewrite_zaful_app_abtest_allindex_gmv_amount_tmp_gmv where af_inner_mediasource='recommend productdetail' and platform='ios') p 
on c.add_time=p.add_time and c.planid=p.planid and c.versionid=p.versionid
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
  i.order_sku_num / c.exp_num,
  j.order_uv / d.sku_uv,
  k.purchase_num,
  l.pay_uv,
  m.pay_amount,
  k.purchase_num / c.exp_num,
  l.pay_uv / d.sku_uv,
  n.collect_uv,
  o.collect_num,
  p.gmv_amount,
  p.gmv_amount / j.order_uv,
  p.gmv_amount / c.exp_num,
  'ios',
  'recommend_homepage',
  c.planid,
  c.versionid
from   
(select exp_num, planid,versionid,add_time from dw_zaful_recommend.rewrite_zaful_app_abtest_allindex_exp_num_tmp_gmv where af_inner_mediasource='recommend_homepage' and platform='ios') c 
left join 
(select sku_uv, planid,versionid,add_time from dw_zaful_recommend.rewrite_zaful_app_abtest_allindex_sku_uv_tmp_gmv where af_inner_mediasource='recommend_homepage' and platform='ios') d 
on c.add_time=d.add_time and c.planid=d.planid and c.versionid=d.versionid
left join 
(select click_num,planid,versionid, add_time from dw_zaful_recommend.rewrite_zaful_app_abtest_allindex_click_num_tmp_gmv where af_inner_mediasource='recommend_homepage' and platform='ios') e 
on c.add_time=e.add_time and c.planid=e.planid and c.versionid=e.versionid
left join 
(select click_uv, planid,versionid,add_time from dw_zaful_recommend.rewrite_zaful_app_abtest_allindex_click_uv_tmp_gmv where af_inner_mediasource='recommend_homepage' and platform='ios') f 
on c.add_time=f.add_time and c.planid=f.planid and c.versionid=f.versionid
left join 
(select cart_num,planid,versionid, add_time from dw_zaful_recommend.rewrite_zaful_app_abtest_allindex_cart_num_tmp_gmv where af_inner_mediasource='recommend_homepage' and platform='ios') g 
on c.add_time=g.add_time and c.planid=g.planid and c.versionid=g.versionid
left join 
(select cart_uv,planid,versionid, add_time from dw_zaful_recommend.rewrite_zaful_app_abtest_allindex_cart_uv_tmp_gmv where af_inner_mediasource='recommend_homepage' and platform='ios') h 
on c.add_time=h.add_time and c.planid=h.planid and c.versionid=h.versionid
left join 
(select order_sku_num, planid,versionid,add_time from dw_zaful_recommend.rewrite_zaful_app_abtest_allindex_order_sku_num_tmp_gmv where af_inner_mediasource='recommend_homepage' and platform='ios') i 
on c.add_time=i.add_time and c.planid=i.planid and c.versionid=i.versionid
left join 
(select order_uv,planid,versionid, add_time from dw_zaful_recommend.rewrite_zaful_app_abtest_allindex_order_uv_tmp_gmv where af_inner_mediasource='recommend_homepage' and platform='ios') j 
on c.add_time=j.add_time and c.planid=j.planid and c.versionid=j.versionid
left join 
(select purchase_num, planid,versionid,add_time from dw_zaful_recommend.rewrite_zaful_app_abtest_allindex_purchase_num_tmp_gmv where af_inner_mediasource='recommend_homepage' and platform='ios') k 
on c.add_time=k.add_time and c.planid=k.planid and c.versionid=k.versionid
left join 
(select pay_uv, planid,versionid,add_time from dw_zaful_recommend.rewrite_zaful_app_abtest_allindex_pay_uv_tmp_gmv where af_inner_mediasource='recommend_homepage' and platform='ios') l 
on c.add_time=l.add_time and c.planid=l.planid and c.versionid=l.versionid
left join 
(select pay_amount,planid,versionid, add_time from dw_zaful_recommend.rewrite_zaful_app_abtest_allindex_pay_amount_tmp_gmv where af_inner_mediasource='recommend_homepage' and platform='ios') m 
on c.add_time=m.add_time and c.planid=m.planid and c.versionid=m.versionid
left join 
(select collect_uv, planid,versionid,add_time from dw_zaful_recommend.rewrite_zaful_app_abtest_allindex_collect_uv_tmp_gmv where af_inner_mediasource='recommend_homepage' and platform='ios') n 
on c.add_time=n.add_time and c.planid=n.planid and c.versionid=n.versionid
left join 
(select collect_num, planid,versionid,add_time from dw_zaful_recommend.rewrite_zaful_app_abtest_allindex_collect_num_tmp_gmv where af_inner_mediasource='recommend_homepage' and platform='ios') o 
on c.add_time=o.add_time and c.planid=o.planid and c.versionid=o.versionid
left join
(select gmv_amount, planid,versionid,add_time from dw_zaful_recommend.rewrite_zaful_app_abtest_allindex_gmv_amount_tmp_gmv where af_inner_mediasource='recommend_homepage' and platform='ios') p 
on c.add_time=p.add_time and c.planid=p.planid and c.versionid=p.versionid
;


--导出表
INSERT into TABLE dw_zaful_recommend.rewrite_zaful_app_abtest_allindex_report_gmv_export 
select 
x.exp_num,
x.sku_uv,
x.click_num,
x.click_uv,
x.exp_click_ratio,
x.user_click_ratio,
x.cart_num,x.cart_uv,
x.sku_cart_ratio,
x.user_cart_ratio,
x.order_sku_num,
x.order_uv,
x.sku_order_ratio,
x.user_order_ratio,
x.purchase_num,
x.pay_uv,
x.pay_amount,
x.sku_purchase_ratio,
x.user_purchase_ratio,
x.collect_uv,
x.collect_num,
x.gmv_amount,
x.gmv_order_ratio,
x.gmv_exp_ratio,
x.platform,
x.af_inner_mediasource,
x.planid,
x.versionid,
x.add_time,
y.order_uv,
y.order_amount,
y.pay_uv,
y.pay_amount
 from
(select * from dw_zaful_recommend.rewrite_zaful_app_abtest_allindex_report_gmv where add_time='${ADD_TIME}') x
left join
(select * from dw_zaful_recommend.rewrite_zaful_app_abtest_orderuv_report where add_time='${ADD_TIME}') y
on x.add_time=y.add_time and x.platform=y.platform and x.planid=y.planid and x.versionid=y.versionid 
;
