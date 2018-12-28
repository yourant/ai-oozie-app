--@author wuchao
--@date 2018年11月24日 
--@desc  RG PC/M推荐位报表

SET mapred.job.name=rosegal_recommend_position_report;
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
CREATE TABLE IF NOT EXISTS dw_proj.rosegal_pc_recommend_position_report(
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
gmv                       INT            COMMENT "GMV",
purchase_num              INT            COMMENT "销量",  
pay_uv                    INT            COMMENT "付款uv",
pay_amount                INT            COMMENT "销售额",
sku_purchase_ratio        decimal(10,4)  COMMENT "商品购买转化率",
user_purchase_ratio       decimal(10,4)  COMMENT "用户购买转化率",
gmv_cost_mille            decimal(10,4)  COMMENT "千次曝光GMV",
collect_uv                INT            COMMENT "商品收藏UV",
collect_num               INT            COMMENT "商品收藏数",
platform                  STRING         COMMENT "平台",
recommend_position        STRING         COMMENT "推荐位编号",
position_name             STRING         COMMENT "推荐位名称",
lang_code                 STRING         COMMENT "语言站",
country                   STRING         COMMENT "国家"
)
COMMENT 'rosegal pc推荐位数据报表'
PARTITIONED BY (add_time STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\u0001' LINES TERMINATED BY '\n' STORED AS TEXTFILE
;


--输出结果导出表
CREATE TABLE IF NOT EXISTS dw_proj.rosegal_pc_recommend_position_report_exp(
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
gmv                       INT            COMMENT "GMV",
purchase_num              INT            COMMENT "销量",  
pay_uv                    INT            COMMENT "付款uv",
pay_amount                INT            COMMENT "销售额",
sku_purchase_ratio        decimal(10,4)  COMMENT "商品购买转化率",
user_purchase_ratio       decimal(10,4)  COMMENT "用户购买转化率",
gmv_cost_mille            decimal(10,4)  COMMENT "千次曝光GMV",
collect_uv                INT            COMMENT "商品收藏UV",
collect_num               INT            COMMENT "商品收藏数",
platform                  STRING         COMMENT "平台",
recommend_position        STRING         COMMENT "推荐位编号",
position_name             STRING         COMMENT "推荐位名称",
lang_code                 STRING         COMMENT "语言站",
country                   STRING         COMMENT "国家",
add_time                  STRING         COMMENT "时间"
)
COMMENT 'rosegal pc推荐位数据报表'
;

--页面PV 
CREATE TABLE IF NOT EXISTS dw_proj.rosegal_pc_report_pv_tmp(
pv                        INT            COMMENT "页面PV",
platform                  STRING         COMMENT "平台",
recommend_position        STRING         COMMENT "推荐位",
lang_code                 STRING         COMMENT "语言站",
country                   STRING         COMMENT "国家",
add_time                  STRING         COMMENT "日期"
)
COMMENT "推荐位报表页面PV中间表"
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\u0001' LINES TERMINATED BY '\n' STORED AS TEXTFILE;

--页面UV	
CREATE TABLE IF NOT EXISTS dw_proj.rosegal_pc_report_uv_tmp(
uv                        INT            COMMENT "页面UV",
platform                  STRING         COMMENT "平台",
recommend_position        STRING         COMMENT "推荐位",
lang_code                 STRING         COMMENT "语言站",
country                   STRING         COMMENT "国家",
add_time                  STRING         COMMENT "日期"
)
COMMENT "推荐位报表页面UV中间表"
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\u0001' LINES TERMINATED BY '\n' STORED AS TEXTFILE;

--商品曝光数	
CREATE TABLE IF NOT EXISTS dw_proj.rosegal_pc_report_exp_num_tmp(
exp_num                   INT            COMMENT "商品曝光数",
platform                  STRING         COMMENT "平台",
recommend_position        STRING         COMMENT "推荐位",
lang_code                 STRING         COMMENT "语言站",
country                   STRING         COMMENT "国家",
add_time                  STRING         COMMENT "日期"
)
COMMENT "推荐位报表商品曝光数中间表"
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\u0001' LINES TERMINATED BY '\n' STORED AS TEXTFILE;

--查看商品UV
CREATE TABLE IF NOT EXISTS dw_proj.rosegal_pc_report_sku_uv_tmp(
sku_uv                    INT            COMMENT "查看商品UV",
platform                  STRING         COMMENT "平台",
recommend_position        STRING         COMMENT "推荐位",
lang_code                 STRING         COMMENT "语言站",
country                   STRING         COMMENT "国家",
add_time                  STRING         COMMENT "日期"
)
COMMENT "推荐位报表查看商品UV中间表"
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\u0001' LINES TERMINATED BY '\n' STORED AS TEXTFILE;

--商品点击数	
CREATE TABLE IF NOT EXISTS dw_proj.rosegal_pc_report_click_num_tmp(
click_num                 INT            COMMENT "商品点击数",
platform                  STRING         COMMENT "平台",
recommend_position        STRING         COMMENT "推荐位",
lang_code                 STRING         COMMENT "语言站",
country                   STRING         COMMENT "国家",
add_time                  STRING         COMMENT "日期"
)
COMMENT "推荐位报表商品点击数中间表"
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\u0001' LINES TERMINATED BY '\n' STORED AS TEXTFILE;

--点击UV
CREATE TABLE IF NOT EXISTS dw_proj.rosegal_pc_report_click_uv_tmp(
click_uv                  INT            COMMENT "点击UV",
platform                  STRING         COMMENT "平台",
recommend_position        STRING         COMMENT "推荐位",
lang_code                 STRING         COMMENT "语言站",
country                   STRING         COMMENT "国家",
add_time                  STRING         COMMENT "日期"
)
COMMENT "推荐位报表点击UV中间表"
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\u0001' LINES TERMINATED BY '\n' STORED AS TEXTFILE;

--商品加购数
CREATE TABLE IF NOT EXISTS dw_proj.rosegal_pc_report_cart_num_tmp(
cart_num                  INT            COMMENT "商品加购数",
platform                  STRING         COMMENT "平台",
recommend_position        STRING         COMMENT "推荐位",
lang_code                 STRING         COMMENT "语言站",
country                   STRING         COMMENT "国家",
add_time                  STRING         COMMENT "日期"
)
COMMENT "推荐位报表商品加购数中间表"
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\u0001' LINES TERMINATED BY '\n' STORED AS TEXTFILE;

--加购UV
CREATE TABLE IF NOT EXISTS dw_proj.rosegal_pc_report_cart_uv_tmp(
cart_uv                   INT            COMMENT "加购UV",
platform                  STRING         COMMENT "平台",
recommend_position        STRING         COMMENT "推荐位",
lang_code                 STRING         COMMENT "语言站",
country                   STRING         COMMENT "国家",
add_time                  STRING         COMMENT "日期"
)
COMMENT "推荐位报表加购uv中间表"
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\u0001' LINES TERMINATED BY '\n' STORED AS TEXTFILE;

--下单商品数
CREATE TABLE IF NOT EXISTS dw_proj.rosegal_pc_report_order_sku_num_tmp(
order_sku_num             INT            COMMENT "下单商品数",
platform                  STRING         COMMENT "平台",
recommend_position        STRING         COMMENT "推荐位",
lang_code                 STRING         COMMENT "语言站",
country                   STRING         COMMENT "国家",
add_time                  STRING         COMMENT "日期"
)
COMMENT "推荐位报表下单商品数中间表"
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\u0001' LINES TERMINATED BY '\n' STORED AS TEXTFILE;

--下单uv
CREATE TABLE IF NOT EXISTS dw_proj.rosegal_pc_report_order_uv_tmp(
order_uv                  INT            COMMENT "下单uv",
platform                  STRING         COMMENT "平台",
recommend_position        STRING         COMMENT "推荐位",
lang_code                 STRING         COMMENT "语言站",
country                   STRING         COMMENT "国家",
add_time                  STRING         COMMENT "日期"
)
COMMENT "推荐位报表下单uv中间表"
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\u0001' LINES TERMINATED BY '\n' STORED AS TEXTFILE;

--gmv
CREATE TABLE IF NOT EXISTS dw_proj.rosegal_pc_report_gmv_tmp(
gmv                       INT            COMMENT "gmv",
platform                  STRING         COMMENT "平台",
recommend_position        STRING         COMMENT "推荐位",
lang_code                 STRING         COMMENT "语言站",
country                   STRING         COMMENT "国家",
add_time                  STRING         COMMENT "日期"
)
COMMENT "推荐位报表下单uv中间表"
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\u0001' LINES TERMINATED BY '\n' STORED AS TEXTFILE;

--销量
CREATE TABLE IF NOT EXISTS dw_proj.rosegal_pc_report_purchase_num_tmp(
purchase_num              INT            COMMENT "销量",
platform                  STRING         COMMENT "平台",
recommend_position        STRING         COMMENT "推荐位",
lang_code                 STRING         COMMENT "语言站",
country                   STRING         COMMENT "国家",
add_time                  STRING         COMMENT "日期"
)
COMMENT "推荐位报表下单uv中间表"
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\u0001' LINES TERMINATED BY '\n' STORED AS TEXTFILE;

--付款uv
CREATE TABLE IF NOT EXISTS dw_proj.rosegal_pc_report_pay_uv_tmp(
pay_uv                    INT            COMMENT "付款uv",
platform                  STRING         COMMENT "平台",
recommend_position        STRING         COMMENT "推荐位",
lang_code                 STRING         COMMENT "语言站",
country                   STRING         COMMENT "国家",
add_time                  STRING         COMMENT "日期"
)
COMMENT "推荐位报表下单uv中间表"
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\u0001' LINES TERMINATED BY '\n' STORED AS TEXTFILE;

--销售额
CREATE TABLE IF NOT EXISTS dw_proj.rosegal_pc_report_pay_amount_tmp(
pay_amount                INT            COMMENT "销售额",
platform                  STRING         COMMENT "平台",
recommend_position        STRING         COMMENT "推荐位",
lang_code                 STRING         COMMENT "语言站",
country                   STRING         COMMENT "国家",
add_time                  STRING         COMMENT "日期"
)
COMMENT "推荐位报表下单uv中间表"
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\u0001' LINES TERMINATED BY '\n' STORED AS TEXTFILE;

--商品收藏uv
CREATE TABLE IF NOT EXISTS dw_proj.rosegal_pc_report_collect_uv_tmp(
collect_uv                INT            COMMENT "商品收藏uv",
platform                  STRING         COMMENT "平台",
recommend_position        STRING         COMMENT "推荐位",
lang_code                 STRING         COMMENT "语言站",
country                   STRING         COMMENT "国家",
add_time                  STRING         COMMENT "日期"
)
COMMENT "推荐位报表下单uv中间表"
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\u0001' LINES TERMINATED BY '\n' STORED AS TEXTFILE;

--商品收藏数
CREATE TABLE IF NOT EXISTS dw_proj.rosegal_pc_report_collect_num_tmp(
collect_num               INT            COMMENT "商品收藏数",
platform                  STRING         COMMENT "平台",
recommend_position        STRING         COMMENT "推荐位",
lang_code                 STRING         COMMENT "语言站",
country                   STRING         COMMENT "国家",
add_time                  STRING         COMMENT "日期"
)
COMMENT "推荐位报表下单uv中间表"
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\u0001' LINES TERMINATED BY '\n' STORED AS TEXTFILE;


--cookie_id,user_id对应表
CREATE TABLE IF NOT EXISTS dw_proj.wuc_rg_od_u_map(
cookie_id               STRING            COMMENT "",
user_id                 STRING            COMMENT "用户id"
)
COMMENT "cookie_id,用户id对应关系表"
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\u0001' LINES TERMINATED BY '\n' STORED AS TEXTFILE;

--cookie_id,user_id对应表
INSERT OVERWRITE TABLE  dw_proj.wuc_rg_od_u_map
SELECT
m.cookie_id,
m.user_id
FROM 
(
  SELECT
  cookie_id,
  user_id
  FROM
  dw_proj.wuc_rg_od_u_map
  union all
  SELECT
  cookie_id,
  user_id
  FROM
  ods.ods_pc_burial_log 
  where 
  concat_ws('-', year, month, day) = '${ADD_TIME}'
      AND cookie_id <> '0'
      AND user_id <> ''
      and site='rosegal'
) m
GROUP BY
m.cookie_id,
m.user_id 
;

--14天加购sku,userid对应关系表
CREATE TABLE IF NOT EXISTS dw_proj.rosegal_pc_report_sku_user_id_tmp(
goods_sn                  STRING         COMMENT "商品id",
user_id                   STRING         COMMENT "用户id",
platform                  STRING         COMMENT "平台",
recommend_position        STRING         COMMENT "推荐位",
lang_code                 STRING         COMMENT "语言站",
country                   STRING         COMMENT "国家"
)
COMMENT "推荐位报表下单sku,user_id中间表"
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\u0001' LINES TERMINATED BY '\n' STORED AS TEXTFILE;


--14天加购sku,userid对应关系表
INSERT OVERWRITE TABLE dw_proj.rosegal_pc_report_sku_user_id_tmp
SELECT
  m.goods_sn,
  n.user_id,
  m.platform,
  m.recommend_position,
  m.country_number,
  m.country_name
FROM
  (
    SELECT
      cookie_id,
      get_json_object(skuinfo,'$.sku') as goods_sn,
      platform,
      get_json_object(sub_event_field,'$.fmd') as recommend_position,
      country_number,
      country_name
    FROM
      ods.ods_pc_burial_log 
    WHERE
      concat_ws('-', year, month, day) BETWEEN '${ADD_TIME_W}'
      AND '${ADD_TIME}'
      and behaviour_type = 'ic'
      and sub_event_info = 'ADT'
      and get_json_object(sub_event_field,'$.fmd') 
      in ('mr_T_2','mr_T_3','mr_T_4','mr_T_5','mr_T_6')
      and site='rosegal'

  ) m
  INNER JOIN dw_proj.wuc_rg_od_u_map n ON m.cookie_id = n.cookie_id
GROUP BY
  m.goods_sn,
  n.user_id,
  m.platform,
  m.recommend_position,
  m.country_number,
  m.country_name
;


--14天加购sku,userid对应关系表  带cookie_id
CREATE TABLE IF NOT EXISTS dw_proj.rosegal_pc_report_cookie_id_sku_user_id_tmp(
cookie_id                 STRING         COMMENT "cookie id",
goods_sn                  STRING         COMMENT "商品id",
user_id                   STRING         COMMENT "用户id",
platform                  STRING         COMMENT "平台",
recommend_position        STRING         COMMENT "推荐位",
lang_code                 STRING         COMMENT "语言站",
country                   STRING         COMMENT "国家"
)
COMMENT "推荐位报表下单cookie_id,sku,user_id中间表"
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\u0001' LINES TERMINATED BY '\n' STORED AS TEXTFILE;


--14天加购sku,userid对应关系表  带cookie_id
INSERT OVERWRITE TABLE dw_proj.rosegal_pc_report_cookie_id_sku_user_id_tmp
SELECT
  m.cookie_id,
  m.goods_sn,
  n.user_id,
  m.platform,
  m.recommend_position,
  m.country_number,
  m.country_name
FROM
  (
    SELECT
      cookie_id,
      get_json_object(skuinfo,'$.sku') as goods_sn,
      platform,
      get_json_object(sub_event_field,'$.fmd') as recommend_position,
      country_number,
      country_name
    FROM
      ods.ods_pc_burial_log 
    WHERE
      concat_ws('-', year, month, day) BETWEEN '${ADD_TIME_W}'
      AND '${ADD_TIME}'
      and behaviour_type = 'ic'
      and sub_event_info = 'ADT'
      and get_json_object(sub_event_field,'$.fmd') 
      in ('mr_T_2','mr_T_3','mr_T_4','mr_T_5','mr_T_6')
      and site='rosegal'

  ) m
  INNER JOIN dw_proj.wuc_rg_od_u_map n ON m.cookie_id = n.cookie_id
GROUP BY
  m.cookie_id,
  m.goods_sn,
  n.user_id,
  m.platform,
  m.recommend_position,
  m.country_number,
  m.country_name
;

--订单信息表
CREATE TABLE IF NOT EXISTS dw_proj.rosegal_pc_report_order_good_info_tmp(
goods_sn                  STRING         COMMENT "sku",
user_id                   STRING         COMMENT "用户ID",
order_status              STRING         COMMENT "订单状态",
pay_amount                decimal(10,2)  COMMENT "购买金额",
goods_number              STRING         COMMENT "商品数量"
)
COMMENT "推荐位报表user-sku中间表"
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\u0001' LINES TERMINATED BY '\n' STORED AS TEXTFILE;

----订单信息表
INSERT OVERWRITE TABLE dw_proj.rosegal_pc_report_order_good_info_tmp
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
      ods.ods_m_rosegal_eload_order_info
    WHERE
      from_unixtime(add_time, 'yyyy-MM-dd') = '${ADD_TIME}'
    group by
     order_id,
     user_id,
     add_time,
     order_status
  ) x
 INNER JOIN
  (
    select
     m.goods_sn,
     m.order_id,
     m.goods_number,
     case when m.discount_price <> '0' or  m.discount_price is not null then m.discount_price * m.goods_number
     else m.goods_price * m.goods_number end as pay_amount
    from
       (
        SELECT 
        goods_sn,
        order_id,
        goods_number,
        discount_price,
        goods_price
        --case when discount_price <> '0' or  discount_price is not null then discount_price * goods_number
        --else goods_price*goods_number end as pay_amount
        from
        ods.ods_m_rosegal_eload_order_goods 
        group by 
        order_id,
        goods_sn,
        goods_number,
        discount_price,
        goods_price
       ) m
   ) p
   ON x.order_id = p.order_id
;

--页面PV
INSERT OVERWRITE TABLE dw_proj.rosegal_pc_report_pv_tmp
SELECT
  count(cookie_id) as pv,
  platform,
  'related_recommendations' as recommend_position,
  country_number,
  country_name,
  '${ADD_TIME}' as add_time
FROM
  ods.ods_pc_burial_log
WHERE
  concat_ws('-', year, month, day) = '${ADD_TIME}'
  and behaviour_type = 'ie'
  and page_main_type = 'c'
  and sub_event_field is null
  and site='rosegal'
group by
  platform,
  country_number,
  country_name
union all
SELECT
  count(cookie_id) as pv,
  platform,
  'customers_also_viewed' as recommend_position,
  country_number,
  country_name,
  '${ADD_TIME}' as add_time
FROM
  ods.ods_pc_burial_log
WHERE
  concat_ws('-', year, month, day) = '${ADD_TIME}'
  and behaviour_type = 'ie'
  and page_main_type = 'c'
  and sub_event_field is null
  and site='rosegal'
group by
  platform,
  country_number,
  country_name
union all
SELECT
  count(cookie_id) as pv,
  platform,
  'featured_recommendations' as recommend_position,
  country_number,
  country_name,
  '${ADD_TIME}' as add_time
FROM
  ods.ods_pc_burial_log
WHERE
  concat_ws('-', year, month, day) = '${ADD_TIME}'
  and behaviour_type = 'ie'
  and page_sub_type = 'd05'
  and sub_event_field is null
  and site='rosegal'
group by
  platform,
  country_number,
  country_name
union all
SELECT
  count(cookie_id) as pv,
  platform,
  'may_be_you_like' as recommend_position,
  country_number,
  country_name,
  '${ADD_TIME}' as add_time
FROM
  ods.ods_pc_burial_log
WHERE
  concat_ws('-', year, month, day) = '${ADD_TIME}'
  and behaviour_type = 'ie'
  and page_sub_type = 'b02'
  and sub_event_field is null
  and get_json_object(page_info,'$.view') = 0
  and site='rosegal'
group by
  platform,
  country_number,
  country_name
;




--页面UV	
INSERT OVERWRITE TABLE dw_proj.rosegal_pc_report_uv_tmp
SELECT
  count(distinct cookie_id) as pv,
  platform,
  'related_recommendations' as recommend_position,
  country_number,
  country_name,
  '${ADD_TIME}' as add_time
FROM
  ods.ods_pc_burial_log
WHERE
  concat_ws('-', year, month, day) = '${ADD_TIME}'
  and behaviour_type = 'ie'
  and page_main_type = 'c'
  and sub_event_field is null
  and site='rosegal'
group by
  platform,
  country_number,
  country_name
union all
SELECT
  count(distinct cookie_id) as pv,
  platform,
  'customers_also_viewed' as recommend_position,
  country_number,
  country_name,
  '${ADD_TIME}' as add_time
FROM
  ods.ods_pc_burial_log
WHERE
  concat_ws('-', year, month, day) = '${ADD_TIME}'
  and behaviour_type = 'ie'
  and page_main_type = 'c'
  and sub_event_field is null
  and site='rosegal'
group by
  platform,
  country_number,
  country_name
union all
SELECT
  count(distinct cookie_id) as pv,
  platform,
  'featured_recommendations' as recommend_position,
  country_number,
  country_name,
  '${ADD_TIME}' as add_time
FROM
  ods.ods_pc_burial_log
WHERE
  concat_ws('-', year, month, day) = '${ADD_TIME}'
  and behaviour_type = 'ie'
  and page_sub_type = 'd05'
  and sub_event_field is null
  and site='rosegal'
group by
  platform,
  country_number,
  country_name
union all
SELECT
  count(distinct cookie_id) as pv,
  platform,
  'may_be_you_like' as recommend_position,
  country_number,
  country_name,
  '${ADD_TIME}' as add_time
FROM
  ods.ods_pc_burial_log
WHERE
  concat_ws('-', year, month, day) = '${ADD_TIME}'
  and behaviour_type = 'ie'
  and page_sub_type = 'b02'
  and sub_event_field is null
  and get_json_object(page_info,'$.view') = 0
  and site='rosegal'
group by
  platform,
  country_number,
  country_name
;

--商品曝光数	
INSERT OVERWRITE TABLE dw_proj.rosegal_pc_report_exp_num_tmp
SELECT
sum(m.exp_num) as exp_num,
m.platform,
m.recommend_position,
m.country_number,
m.country_name,
'${ADD_TIME}' as add_time
FROM
 (
    SELECT
    length(get_json_object(x.sku, '$.sku'))-length(regexp_replace(get_json_object(x.sku, '$.sku'),',',''))+1 as exp_num,
    x.platform,
    get_json_object(x.sku,'$.mrlc') as recommend_position,
    x.country_number,
    x.country_name
    FROM
    (
      SELECT 
      sku,
      n.platform,
      n.country_number,
      n.country_name
      FROM
      (
        SELECT
          platform,
          regexp_replace(regexp_extract(sub_event_field ,'^\\[(.+)\\]$',1),'\\}\\,\\{','\\}\\|\\|\\{') as skus,
          country_number,
          country_name
        FROM
          ods.ods_pc_burial_log
        WHERE
          concat_ws('-', year, month, day) = '${ADD_TIME}'
          and behaviour_type = 'ie'
          and page_main_type = 'c'
          and page_module = 'mr'
          --and get_json_object(regexp_replace(regexp_extract(sub_event_field ,'^\\[(.+)\\]$',1),'\\}\\,\\{(.+)\\}','}'),'$.mrlc') in ('T_2','T_3','T_4','T_5','T_6')
          and site='rosegal'
      ) n
        LATERAL VIEW explode(split(n.skus,'\\|\\|')) zqms as sku
    ) x
    where get_json_object(x.sku,'$.mrlc') in ('T_2','T_3','T_4','T_5','T_6')
 ) m
group by
  m.platform,
  m.recommend_position,
  m.country_number,
  m.country_name

union all

SELECT
sum(m.exp_num) as exp_num,
m.platform,
m.recommend_position,
m.country_number,
m.country_name,
'${ADD_TIME}' as add_time
FROM
 (
    SELECT
    length(get_json_object(x.sku, '$.sku'))-length(regexp_replace(get_json_object(x.sku, '$.sku'),',',''))+1 as exp_num,
    x.platform,
    get_json_object(x.sku,'$.mrlc') as recommend_position,
    x.country_number,
    x.country_name
    FROM
    (
      SELECT 
      sku,
      n.platform,
      n.country_number,
      n.country_name
      FROM
      (
        SELECT
          platform,
          regexp_replace(regexp_extract(sub_event_field ,'^\\[(.+)\\]$',1),'\\}\\,\\{','\\}\\|\\|\\{') as skus,
          country_number,
          country_name
        FROM
          ods.ods_pc_burial_log
        WHERE
          concat_ws('-', year, month, day) = '${ADD_TIME}'
          and behaviour_type = 'ie'
          and page_main_type = 'd'
          and page_module = 'mr'
          --and get_json_object(regexp_replace(regexp_extract(sub_event_field ,'^\\[(.+)\\]$',1),'\\}\\,\\{(.+)\\}','}'),'$.mrlc') in ('T_2','T_3','T_4','T_5','T_6')
          and site='rosegal'
      ) n
        LATERAL VIEW explode(split(n.skus,'\\|\\|')) zqms as sku
    ) x
    where get_json_object(x.sku,'$.mrlc') in ('T_2','T_3','T_4','T_5','T_6')
 ) m
group by
  m.platform,
  m.recommend_position,
  m.country_number,
  m.country_name

union all 

SELECT
sum(m.exp_num) as exp_num,
m.platform,
m.recommend_position,
m.country_number,
m.country_name,
'${ADD_TIME}' as add_time
FROM
 (
    SELECT
    length(get_json_object(x.sku, '$.sku'))-length(regexp_replace(get_json_object(x.sku, '$.sku'),',',''))+1 as exp_num,
    x.platform,
    get_json_object(x.sku,'$.mrlc') as recommend_position,
    x.country_number,
    x.country_name
    FROM
    (
      SELECT 
      sku,
      n.platform,
      n.country_number,
      n.country_name
      FROM
      (
        SELECT
          platform,
          regexp_replace(regexp_extract(sub_event_field ,'^\\[(.+)\\]$',1),'\\}\\,\\{','\\}\\|\\|\\{') as skus,
          country_number,
          country_name
        FROM
          ods.ods_pc_burial_log
        WHERE
          concat_ws('-', year, month, day) = '${ADD_TIME}'
          and behaviour_type = 'ie'
          and page_sub_type = 'b02'
          and page_module = 'mr'
          --and get_json_object(regexp_replace(regexp_extract(sub_event_field ,'^\\[(.+)\\]$',1),'\\}\\,\\{(.+)\\}','}'),'$.mrlc') in ('T_2','T_3','T_4','T_5','T_6')
          and site='rosegal'
      ) n
        LATERAL VIEW explode(split(n.skus,'\\|\\|')) zqms as sku
    ) x
    where get_json_object(x.sku,'$.mrlc') in ('T_2','T_3','T_4','T_5','T_6')
 ) m
group by
  m.platform,
  m.recommend_position,
  m.country_number,
  m.country_name
;



--查看商品UV
INSERT OVERWRITE TABLE dw_proj.rosegal_pc_report_sku_uv_tmp
SELECT
count(distinct m.cookie_id) as sku_uv,
m.platform,
m.recommend_position,
m.country_number,
m.country_name,
'${ADD_TIME}' as add_time
FROM
 (
    SELECT
    x.cookie_id,
    x.platform,
    get_json_object(x.sku,'$.mrlc') as recommend_position,
    x.country_number,
    x.country_name
    FROM
    (
      SELECT 
      sku,
      n.cookie_id,
      n.platform,
      n.country_number,
      n.country_name
      FROM
      (
        SELECT
          cookie_id,
          platform,
          regexp_replace(regexp_extract(sub_event_field ,'^\\[(.+)\\]$',1),'\\}\\,\\{','\\}\\|\\|\\{') as skus,
          country_number,
          country_name
        FROM
          ods.ods_pc_burial_log
        WHERE
          concat_ws('-', year, month, day) = '${ADD_TIME}'
          and behaviour_type = 'ie'
          and page_main_type = 'c'
          and page_module = 'mr'
          --and get_json_object(regexp_replace(regexp_extract(sub_event_field ,'^\\[(.+)\\]$',1),'\\}\\,\\{(.+)\\}','}'),'$.mrlc') in ('T_2','T_3','T_4','T_5','T_6')
          and site='rosegal'
      ) n
        LATERAL VIEW explode(split(n.skus,'\\|\\|')) zqms as sku
    ) x
    where get_json_object(x.sku,'$.mrlc') in ('T_2','T_3','T_4','T_5','T_6')
 ) m
group by
  m.platform,
  m.recommend_position,
  m.country_number,
  m.country_name

union all

SELECT
count(distinct m.cookie_id) as sku_uv,
m.platform,
m.recommend_position,
m.country_number,
m.country_name,
'${ADD_TIME}' as add_time
FROM
 (
    SELECT
    x.cookie_id,
    x.platform,
    get_json_object(x.sku,'$.mrlc') as recommend_position,
    x.country_number,
    x.country_name
    FROM
    (
      SELECT 
      sku,
      n.cookie_id,
      n.platform,
      n.country_number,
      n.country_name
      FROM
      (
        SELECT
          cookie_id,
          platform,
          regexp_replace(regexp_extract(sub_event_field ,'^\\[(.+)\\]$',1),'\\}\\,\\{','\\}\\|\\|\\{') as skus,
          country_number,
          country_name
        FROM
          ods.ods_pc_burial_log
        WHERE
          concat_ws('-', year, month, day) = '${ADD_TIME}'
          and behaviour_type = 'ie'
          and page_main_type = 'd'
          and page_module = 'mr'
          --and get_json_object(regexp_replace(regexp_extract(sub_event_field ,'^\\[(.+)\\]$',1),'\\}\\,\\{(.+)\\}','}'),'$.mrlc') in ('T_2','T_3','T_4','T_5','T_6')
          and site='rosegal'
      ) n
        LATERAL VIEW explode(split(n.skus,'\\|\\|')) zqms as sku
    ) x
    where get_json_object(x.sku,'$.mrlc') in ('T_2','T_3','T_4','T_5','T_6')
 ) m
group by
  m.platform,
  m.recommend_position,
  m.country_number,
  m.country_name

union all

SELECT
count(distinct m.cookie_id) as sku_uv,
m.platform,
m.recommend_position,
m.country_number,
m.country_name,
'${ADD_TIME}' as add_time
FROM
 (
    SELECT
    x.cookie_id,
    x.platform,
    get_json_object(x.sku,'$.mrlc') as recommend_position,
    x.country_number,
    x.country_name
    FROM
    (
      SELECT 
      sku,
      n.cookie_id,
      n.platform,
      n.country_number,
      n.country_name
      FROM
      (
        SELECT
          cookie_id,
          platform,
          regexp_replace(regexp_extract(sub_event_field ,'^\\[(.+)\\]$',1),'\\}\\,\\{','\\}\\|\\|\\{') as skus,
          country_number,
          country_name
        FROM
          ods.ods_pc_burial_log
        WHERE
          concat_ws('-', year, month, day) = '${ADD_TIME}'
          and behaviour_type = 'ie'
          and page_sub_type = 'b02'
          and page_module = 'mr'
          --and get_json_object(regexp_replace(regexp_extract(sub_event_field ,'^\\[(.+)\\]$',1),'\\}\\,\\{(.+)\\}','}'),'$.mrlc') in ('T_2','T_3','T_4','T_5','T_6')
          and site='rosegal'
      ) n
        LATERAL VIEW explode(split(n.skus,'\\|\\|')) zqms as sku
    ) x
    where get_json_object(x.sku,'$.mrlc') in ('T_2','T_3','T_4','T_5','T_6')
 ) m
group by
  m.platform,
  m.recommend_position,
  m.country_number,
  m.country_name
;


--商品点击数	
INSERT OVERWRITE TABLE dw_proj.rosegal_pc_report_click_num_tmp
SELECT
count(cookie_id) as click_num,
m.platform,
m.recommend_position,
m.country_number,
m.country_name,
'${ADD_TIME}' as add_time
FROM
 (
    SELECT
      cookie_id,
      platform,
      get_json_object(sub_event_field,'$.mrlc')  as recommend_position,
      country_number,
      country_name 
    FROM
      ods.ods_pc_burial_log
    WHERE
      concat_ws('-', year, month, day) = '${ADD_TIME}'
      and behaviour_type = 'ic'
      and page_main_type = 'c'
      and page_module = 'mr'
      and sub_event_info = 'sku'
      and get_json_object(sub_event_field,'$.mrlc') 
      in ('T_2','T_3','T_4','T_5','T_6')
      and site='rosegal'
 ) m
group by
  m.platform,
  m.recommend_position,
  m.country_number,
  m.country_name

union all 

SELECT
count(cookie_id) as click_num,
m.platform,
m.recommend_position,
m.country_number,
m.country_name,
'${ADD_TIME}' as add_time
FROM
 (
    SELECT
      cookie_id,
      platform,
      get_json_object(sub_event_field,'$.mrlc')  as recommend_position,
      country_number,
      country_name 
    FROM
      ods.ods_pc_burial_log
    WHERE
      concat_ws('-', year, month, day) = '${ADD_TIME}'
      and behaviour_type = 'ic'
      and page_main_type = 'd'
      and page_module = 'mr'
      and sub_event_info = 'sku'
      and get_json_object(sub_event_field,'$.mrlc') 
      in ('T_2','T_3','T_4','T_5','T_6')
      and site='rosegal'
 ) m
group by
  m.platform,
  m.recommend_position,
  m.country_number,
  m.country_name

union all

SELECT
count(cookie_id) as click_num,
m.platform,
m.recommend_position,
m.country_number,
m.country_name,
'${ADD_TIME}' as add_time
FROM
 (
    SELECT
      cookie_id,
      platform,
      get_json_object(sub_event_field,'$.mrlc')  as recommend_position,
      country_number,
      country_name 
    FROM
      ods.ods_pc_burial_log
    WHERE
      concat_ws('-', year, month, day) = '${ADD_TIME}'
      and behaviour_type = 'ic'
      and page_sub_type = 'b02'
      and page_module = 'mr'
      and sub_event_info = 'sku'
      and get_json_object(sub_event_field,'$.mrlc') 
      in ('T_2','T_3','T_4','T_5','T_6')
      and site='rosegal'
 ) m
group by
  m.platform,
  m.recommend_position,
  m.country_number,
  m.country_name
;

--点击UV
INSERT OVERWRITE TABLE dw_proj.rosegal_pc_report_click_uv_tmp
SELECT
count(distinct cookie_id) as click_uv,
m.platform,
m.recommend_position,
m.country_number,
m.country_name,
'${ADD_TIME}' as add_time
FROM
 (
    SELECT
      cookie_id,
      platform,
      get_json_object(sub_event_field,'$.mrlc')  as recommend_position,
      country_number,
      country_name
    FROM
      ods.ods_pc_burial_log
    WHERE
      concat_ws('-', year, month, day) = '${ADD_TIME}'
      and behaviour_type = 'ic'
      and page_main_type = 'c'
      and page_module = 'mr'
      and sub_event_info = 'sku'
      and get_json_object(sub_event_field,'$.mrlc') 
      in ('T_2','T_3','T_4','T_5','T_6')
      and site='rosegal'
 ) m
group by
  m.platform,
  m.recommend_position,
  m.country_number,
  m.country_name

union all

SELECT
count(distinct cookie_id) as click_uv,
m.platform,
m.recommend_position,
m.country_number,
m.country_name,
'${ADD_TIME}' as add_time
FROM
 (
    SELECT
      cookie_id,
      platform,
      get_json_object(sub_event_field,'$.mrlc')  as recommend_position,
      country_number,
      country_name
    FROM
      ods.ods_pc_burial_log
    WHERE
      concat_ws('-', year, month, day) = '${ADD_TIME}'
      and behaviour_type = 'ic'
      and page_main_type = 'd'
      and page_module = 'mr'
      and sub_event_info = 'sku'
      and get_json_object(sub_event_field,'$.mrlc') 
      in ('T_2','T_3','T_4','T_5','T_6')
      and site='rosegal'
 ) m
group by
  m.platform,
  m.recommend_position,
  m.country_number,
  m.country_name

union all

SELECT
count(distinct cookie_id) as click_uv,
m.platform,
m.recommend_position,
m.country_number,
m.country_name,
'${ADD_TIME}' as add_time
FROM
 (
    SELECT
      cookie_id,
      platform,
      get_json_object(sub_event_field,'$.mrlc')  as recommend_position,
      country_number,
      country_name
    FROM
      ods.ods_pc_burial_log
    WHERE
      concat_ws('-', year, month, day) = '${ADD_TIME}'
      and behaviour_type = 'ic'
      and page_sub_type = 'b02'
      and page_module = 'mr'
      and sub_event_info = 'sku'
      and get_json_object(sub_event_field,'$.mrlc') 
      in ('T_2','T_3','T_4','T_5','T_6')
      and site='rosegal'
 ) m
group by
  m.platform,
  m.recommend_position,
  m.country_number,
  m.country_name
;


--商品加购数
INSERT OVERWRITE TABLE dw_proj.rosegal_pc_report_cart_num_tmp
SELECT
sum(pam) as cart_num,
m.platform,
m.recommend_position,
m.country_number,
m.country_name,
'${ADD_TIME}' as add_time
FROM
 (
    SELECT
      get_json_object(skuinfo,'$.pam') as pam,
      platform,
      get_json_object(sub_event_field,'$.fmd') as recommend_position,
      country_number,
      country_name
    FROM
      ods.ods_pc_burial_log
    WHERE
      concat_ws('-', year, month, day) = '${ADD_TIME}'
      and behaviour_type = 'ic'
      and sub_event_info = 'ADT'
      and get_json_object(sub_event_field,'$.fmd') 
      in ('mr_T_2','mr_T_3','mr_T_4','mr_T_5','mr_T_6')
      and site='rosegal'
 ) m
group by
  m.platform,
  m.recommend_position,
  m.country_number,
  m.country_name
;

--加购UV
INSERT OVERWRITE TABLE dw_proj.rosegal_pc_report_cart_uv_tmp
SELECT
count(distinct cookie_id) as cart_uv,
m.platform,
m.recommend_position,
m.country_number,
m.country_name,
'${ADD_TIME}' as add_time
FROM
 (
    SELECT
      cookie_id,
      platform,
      get_json_object(sub_event_field,'$.fmd') as recommend_position,
      country_number,
      country_name
    FROM
      ods.ods_pc_burial_log
    WHERE
      concat_ws('-', year, month, day) = '${ADD_TIME}'
      and behaviour_type = 'ic'
      and sub_event_info = 'ADT'
      and get_json_object(sub_event_field,'$.fmd') 
      in ('mr_T_2','mr_T_3','mr_T_4','mr_T_5','mr_T_6')
      and site='rosegal'
 ) m
group by
  m.platform,
  m.recommend_position,
  m.country_number,
  m.country_name
;


--下单商品数
INSERT OVERWRITE TABLE dw_proj.rosegal_pc_report_order_sku_num_tmp
SELECT
  SUM(x2.goods_number) AS order_sku_num,
  x1.platform,
  x1.recommend_position,
  x1.lang_code,
  x1.country,
  '${ADD_TIME}' as add_time
from
  dw_proj.rosegal_pc_report_sku_user_id_tmp x1
  INNER JOIN dw_proj.rosegal_pc_report_order_good_info_tmp x2 ON x1.user_id = x2.user_id
  AND x1.goods_sn = x2.goods_sn
group by
  x1.platform,
  x1.recommend_position,
  x1.lang_code,
  x1.country
;

--下单uv
INSERT OVERWRITE TABLE dw_proj.rosegal_pc_report_order_uv_tmp
SELECT
  count(distinct x1.cookie_id) as order_uv,
  x1.platform,
  x1.recommend_position,
  x1.lang_code,
  x1.country,
  '${ADD_TIME}' as add_time
from
  dw_proj.rosegal_pc_report_cookie_id_sku_user_id_tmp x1
  INNER JOIN dw_proj.rosegal_pc_report_order_good_info_tmp x2 ON x1.user_id = x2.user_id
  AND x1.goods_sn = x2.goods_sn
group by
  x1.platform,
  x1.recommend_position,
  x1.lang_code,
  x1.country
;

--gmv
INSERT OVERWRITE TABLE dw_proj.rosegal_pc_report_gmv_tmp
SELECT
  SUM(x2.pay_amount) AS gmv,
  x1.platform,
  x1.recommend_position,
  x1.lang_code,
  x1.country,
  '${ADD_TIME}' as add_time
from
  dw_proj.rosegal_pc_report_sku_user_id_tmp x1
  INNER JOIN dw_proj.rosegal_pc_report_order_good_info_tmp x2 ON x1.user_id = x2.user_id
  AND x1.goods_sn = x2.goods_sn
group by
  x1.platform,
  x1.recommend_position,
  x1.lang_code,
  x1.country
; 

--销量
INSERT OVERWRITE TABLE dw_proj.rosegal_pc_report_purchase_num_tmp
SELECT
  SUM(x2.goods_number) AS purchase_num,
  x1.platform,
  x1.recommend_position,
  x1.lang_code,
  x1.country,
  '${ADD_TIME}' as add_time
from
  dw_proj.rosegal_pc_report_sku_user_id_tmp x1
  INNER JOIN dw_proj.rosegal_pc_report_order_good_info_tmp x2 ON x1.user_id = x2.user_id
  AND x1.goods_sn = x2.goods_sn
where
  x2.order_status not in ('0','10','11','12')
group by
  x1.platform,
  x1.recommend_position,
  x1.lang_code,
  x1.country
;

--付款uv
INSERT OVERWRITE TABLE dw_proj.rosegal_pc_report_pay_uv_tmp
SELECT
  count(distinct x1.cookie_id) as pay_uv,
  x1.platform,
  x1.recommend_position,
  x1.lang_code,
  x1.country,
  '${ADD_TIME}' as add_time
from
  dw_proj.rosegal_pc_report_cookie_id_sku_user_id_tmp x1
  INNER JOIN dw_proj.rosegal_pc_report_order_good_info_tmp x2 ON x1.user_id = x2.user_id
  AND x1.goods_sn = x2.goods_sn
where
  x2.order_status not in ('0','10','11','12')
group by
  x1.platform,
  x1.recommend_position,
  x1.lang_code,
  x1.country
;

--销售额
INSERT OVERWRITE TABLE dw_proj.rosegal_pc_report_pay_amount_tmp
SELECT
  SUM(x2.pay_amount) AS pay_amount,
  x1.platform,
  x1.recommend_position,
  x1.lang_code,
  x1.country,
  '${ADD_TIME}' as add_time
from
  dw_proj.rosegal_pc_report_sku_user_id_tmp x1
  INNER JOIN dw_proj.rosegal_pc_report_order_good_info_tmp x2 ON x1.user_id = x2.user_id
  AND x1.goods_sn = x2.goods_sn
where
  x2.order_status not in ('0','10','11','12')
group by
  x1.platform,
  x1.recommend_position,
  x1.lang_code,
  x1.country
; 

--商品收藏uv
INSERT OVERWRITE TABLE dw_proj.rosegal_pc_report_collect_uv_tmp
SELECT
count(distinct cookie_id) as collect_uv,
m.platform,
m.recommend_position,
m.country_number,
m.country_name,
'${ADD_TIME}' as add_time
FROM
 (
    SELECT
      cookie_id,
      platform,
      get_json_object(sub_event_field,'$.fmd') as recommend_position,
      country_number,
      country_name
    FROM
      ods.ods_pc_burial_log
    WHERE
      concat_ws('-', year, month, day) = '${ADD_TIME}'
      and behaviour_type = 'ic'
      and sub_event_info = 'ADF'
      and user_id is not null
      and get_json_object(sub_event_field,'$.fmd') 
      in ('mr_T_2','mr_T_3','mr_T_4','mr_T_5','mr_T_6')
      and site='rosegal'
 ) m
group by
  m.platform,
  m.recommend_position,
  m.country_number,
  m.country_name
;

--商品收藏数
INSERT OVERWRITE TABLE dw_proj.rosegal_pc_report_collect_num_tmp
SELECT
count(cookie_id) as collect_num,
m.platform,
m.recommend_position,
m.country_number,
m.country_name,
'${ADD_TIME}' as add_time
FROM
 (
    SELECT
      cookie_id,
      platform,
      get_json_object(sub_event_field,'$.fmd') as recommend_position,
      country_number,
      country_name
    FROM
      ods.ods_pc_burial_log
    WHERE
      concat_ws('-', year, month, day) = '${ADD_TIME}'
      and behaviour_type = 'ic'
      and sub_event_info = 'ADF'
      and user_id is not null
      and get_json_object(sub_event_field,'$.fmd') 
      in ('mr_T_2','mr_T_3','mr_T_4','mr_T_5','mr_T_6')
      and site='rosegal'
 ) m
group by
  m.platform,
  m.recommend_position,
  m.country_number,
  m.country_name
;


--所有结果汇总
INSERT OVERWRITE TABLE dw_proj.rosegal_pc_recommend_position_report PARTITION (add_time = '${ADD_TIME}')
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
  h.cart_uv / d.sku_uv * 100,
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
  'pc',
  '商详页推荐T_3',
  '商详页推荐-related_recommendations',
  a.lang_code,
  a.country
from  
(select pv,add_time,lang_code,country from  dw_proj.rosegal_pc_report_pv_tmp where platform='pc' and recommend_position='related_recommendations' ) a 
left join 
(select uv,add_time,lang_code,country from  dw_proj.rosegal_pc_report_uv_tmp where platform='pc' and recommend_position='related_recommendations' ) b
on a.add_time=b.add_time and  a.lang_code=b.lang_code and a.country=b.country
left join
(select exp_num,add_time,lang_code,country from  dw_proj.rosegal_pc_report_exp_num_tmp where platform='pc' and recommend_position='T_3' ) c
on a.add_time=c.add_time and  a.lang_code=c.lang_code and a.country=c.country
left join
(select sku_uv,add_time,lang_code,country from  dw_proj.rosegal_pc_report_sku_uv_tmp where platform='pc' and recommend_position='T_3' ) d
on a.add_time=d.add_time and  a.lang_code=d.lang_code and a.country=d.country
left join
(select click_num,add_time,lang_code,country from  dw_proj.rosegal_pc_report_click_num_tmp where platform='pc' and recommend_position='T_3' ) e
on a.add_time=e.add_time and  a.lang_code=e.lang_code and a.country=e.country
left join
(select click_uv,add_time,lang_code,country from  dw_proj.rosegal_pc_report_click_uv_tmp where platform='pc' and recommend_position='T_3' ) f
on a.add_time=f.add_time and  a.lang_code=f.lang_code and a.country=f.country
left join
(select cart_num,add_time,lang_code,country from  dw_proj.rosegal_pc_report_cart_num_tmp where platform='pc' and recommend_position='mr_T_3' ) g
on a.add_time=g.add_time and  a.lang_code=g.lang_code and a.country=g.country
left join
(select cart_uv,add_time,lang_code,country from  dw_proj.rosegal_pc_report_cart_uv_tmp where platform='pc' and recommend_position='mr_T_3' ) h
on a.add_time=h.add_time and  a.lang_code=h.lang_code and a.country=h.country
left join
(select order_sku_num,add_time,lang_code,country from  dw_proj.rosegal_pc_report_order_sku_num_tmp where platform='pc' and recommend_position='mr_T_3' ) i
on a.add_time=i.add_time and  a.lang_code=i.lang_code and a.country=i.country
left join
(select order_uv,add_time,lang_code,country from  dw_proj.rosegal_pc_report_order_uv_tmp where platform='pc' and recommend_position='mr_T_3' ) j
on a.add_time=j.add_time and  a.lang_code=j.lang_code and a.country=j.country
left join
(select gmv,add_time,lang_code,country from  dw_proj.rosegal_pc_report_gmv_tmp where platform='pc' and recommend_position='mr_T_3' ) k
on a.add_time=k.add_time and  a.lang_code=k.lang_code and a.country=k.country
left join
(select purchase_num,add_time,lang_code,country from  dw_proj.rosegal_pc_report_purchase_num_tmp where platform='pc' and recommend_position='mr_T_3' ) l
on a.add_time=l.add_time and  a.lang_code=l.lang_code and a.country=l.country
left join
(select pay_uv,add_time,lang_code,country from  dw_proj.rosegal_pc_report_pay_uv_tmp where platform='pc' and recommend_position='mr_T_3' ) m
on a.add_time=m.add_time and  a.lang_code=m.lang_code and a.country=m.country
left join
(select pay_amount,add_time,lang_code,country from  dw_proj.rosegal_pc_report_pay_amount_tmp where platform='pc' and recommend_position='mr_T_3' ) n
on a.add_time=n.add_time and  a.lang_code=n.lang_code and a.country=n.country
left join
(select collect_uv,add_time,lang_code,country from  dw_proj.rosegal_pc_report_collect_uv_tmp where platform='pc' and recommend_position='mr_T_3' ) o
on a.add_time=o.add_time and  a.lang_code=o.lang_code and a.country=o.country
left join
(select collect_num,add_time,lang_code,country from  dw_proj.rosegal_pc_report_collect_num_tmp where platform='pc' and recommend_position='mr_T_3' ) p
on a.add_time=p.add_time and  a.lang_code=p.lang_code and a.country=p.country

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
  h.cart_uv / d.sku_uv * 100,
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
  'pc',
  '商详页推荐T_4',
  '商详页推荐-customers_also_viewed',
  a.lang_code,
  a.country
from  
(select pv,add_time,lang_code,country from  dw_proj.rosegal_pc_report_pv_tmp where platform='pc' and recommend_position='customers_also_viewed' ) a 
left join 
(select uv,add_time,lang_code,country from  dw_proj.rosegal_pc_report_uv_tmp where platform='pc' and recommend_position='customers_also_viewed' ) b
on a.add_time=b.add_time and  a.lang_code=b.lang_code and a.country=b.country
left join
(select exp_num,add_time,lang_code,country from  dw_proj.rosegal_pc_report_exp_num_tmp where platform='pc' and recommend_position='T_4' ) c
on a.add_time=c.add_time and  a.lang_code=c.lang_code and a.country=c.country
left join
(select sku_uv,add_time,lang_code,country from  dw_proj.rosegal_pc_report_sku_uv_tmp where platform='pc' and recommend_position='T_4' ) d
on a.add_time=d.add_time and  a.lang_code=d.lang_code and a.country=d.country
left join
(select click_num,add_time,lang_code,country from  dw_proj.rosegal_pc_report_click_num_tmp where platform='pc' and recommend_position='T_4' ) e
on a.add_time=e.add_time and  a.lang_code=e.lang_code and a.country=e.country
left join
(select click_uv,add_time,lang_code,country from  dw_proj.rosegal_pc_report_click_uv_tmp where platform='pc' and recommend_position='T_4' ) f
on a.add_time=f.add_time and  a.lang_code=f.lang_code and a.country=f.country
left join
(select cart_num,add_time,lang_code,country from  dw_proj.rosegal_pc_report_cart_num_tmp where platform='pc' and recommend_position='mr_T_4' ) g
on a.add_time=g.add_time and  a.lang_code=g.lang_code and a.country=g.country
left join
(select cart_uv,add_time,lang_code,country from  dw_proj.rosegal_pc_report_cart_uv_tmp where platform='pc' and recommend_position='mr_T_4' ) h
on a.add_time=h.add_time and  a.lang_code=h.lang_code and a.country=h.country
left join
(select order_sku_num,add_time,lang_code,country from  dw_proj.rosegal_pc_report_order_sku_num_tmp where platform='pc' and recommend_position='mr_T_4' ) i
on a.add_time=i.add_time and  a.lang_code=i.lang_code and a.country=i.country
left join
(select order_uv,add_time,lang_code,country from  dw_proj.rosegal_pc_report_order_uv_tmp where platform='pc' and recommend_position='mr_T_4' ) j
on a.add_time=j.add_time and  a.lang_code=j.lang_code and a.country=j.country
left join
(select gmv,add_time,lang_code,country from  dw_proj.rosegal_pc_report_gmv_tmp where platform='pc' and recommend_position='mr_T_4' ) k
on a.add_time=k.add_time and  a.lang_code=k.lang_code and a.country=k.country
left join
(select purchase_num,add_time,lang_code,country from  dw_proj.rosegal_pc_report_purchase_num_tmp where platform='pc' and recommend_position='mr_T_4' ) l
on a.add_time=l.add_time and  a.lang_code=l.lang_code and a.country=l.country
left join
(select pay_uv,add_time,lang_code,country from  dw_proj.rosegal_pc_report_pay_uv_tmp where platform='pc' and recommend_position='mr_T_4' ) m
on a.add_time=m.add_time and  a.lang_code=m.lang_code and a.country=m.country
left join
(select pay_amount,add_time,lang_code,country from  dw_proj.rosegal_pc_report_pay_amount_tmp where platform='pc' and recommend_position='mr_T_4' ) n
on a.add_time=n.add_time and  a.lang_code=n.lang_code and a.country=n.country
left join
(select collect_uv,add_time,lang_code,country from  dw_proj.rosegal_pc_report_collect_uv_tmp where platform='pc' and recommend_position='mr_T_4' ) o
on a.add_time=o.add_time and  a.lang_code=o.lang_code and a.country=o.country
left join
(select collect_num,add_time,lang_code,country from  dw_proj.rosegal_pc_report_collect_num_tmp where platform='pc' and recommend_position='mr_T_4' ) p
on a.add_time=p.add_time and  a.lang_code=p.lang_code and a.country=p.country

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
  h.cart_uv / d.sku_uv * 100,
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
  'pc',
  '空购物车页T_5',
  '空购物车页-featured_recommendations',
  a.lang_code,
  a.country
from  
(select pv,add_time,lang_code,country from  dw_proj.rosegal_pc_report_pv_tmp where platform='pc' and recommend_position='featured_recommendations' ) a 
left join 
(select uv,add_time,lang_code,country from  dw_proj.rosegal_pc_report_uv_tmp where platform='pc' and recommend_position='featured_recommendations' ) b
on a.add_time=b.add_time and  a.lang_code=b.lang_code and a.country=b.country
left join
(select exp_num,add_time,lang_code,country from  dw_proj.rosegal_pc_report_exp_num_tmp where platform='pc' and recommend_position='T_5' ) c
on a.add_time=c.add_time and  a.lang_code=c.lang_code and a.country=c.country
left join
(select sku_uv,add_time,lang_code,country from  dw_proj.rosegal_pc_report_sku_uv_tmp where platform='pc' and recommend_position='T_5' ) d
on a.add_time=d.add_time and  a.lang_code=d.lang_code and a.country=d.country
left join
(select click_num,add_time,lang_code,country from  dw_proj.rosegal_pc_report_click_num_tmp where platform='pc' and recommend_position='T_5' ) e
on a.add_time=e.add_time and  a.lang_code=e.lang_code and a.country=e.country
left join
(select click_uv,add_time,lang_code,country from  dw_proj.rosegal_pc_report_click_uv_tmp where platform='pc' and recommend_position='T_5' ) f
on a.add_time=f.add_time and  a.lang_code=f.lang_code and a.country=f.country
left join
(select cart_num,add_time,lang_code,country from  dw_proj.rosegal_pc_report_cart_num_tmp where platform='pc' and recommend_position='mr_T_5' ) g
on a.add_time=g.add_time and  a.lang_code=g.lang_code and a.country=g.country
left join
(select cart_uv,add_time,lang_code,country from  dw_proj.rosegal_pc_report_cart_uv_tmp where platform='pc' and recommend_position='mr_T_5' ) h
on a.add_time=h.add_time and  a.lang_code=h.lang_code and a.country=h.country
left join
(select order_sku_num,add_time,lang_code,country from  dw_proj.rosegal_pc_report_order_sku_num_tmp where platform='pc' and recommend_position='mr_T_5' ) i
on a.add_time=i.add_time and  a.lang_code=i.lang_code and a.country=i.country
left join
(select order_uv,add_time,lang_code,country from  dw_proj.rosegal_pc_report_order_uv_tmp where platform='pc' and recommend_position='mr_T_5' ) j
on a.add_time=j.add_time and  a.lang_code=j.lang_code and a.country=j.country
left join
(select gmv,add_time,lang_code,country from  dw_proj.rosegal_pc_report_gmv_tmp where platform='pc' and recommend_position='mr_T_5' ) k
on a.add_time=k.add_time and  a.lang_code=k.lang_code and a.country=k.country
left join
(select purchase_num,add_time,lang_code,country from  dw_proj.rosegal_pc_report_purchase_num_tmp where platform='pc' and recommend_position='mr_T_5' ) l
on a.add_time=l.add_time and  a.lang_code=l.lang_code and a.country=l.country
left join
(select pay_uv,add_time,lang_code,country from  dw_proj.rosegal_pc_report_pay_uv_tmp where platform='pc' and recommend_position='mr_T_5' ) m
on a.add_time=m.add_time and  a.lang_code=m.lang_code and a.country=m.country
left join
(select pay_amount,add_time,lang_code,country from  dw_proj.rosegal_pc_report_pay_amount_tmp where platform='pc' and recommend_position='mr_T_5' ) n
on a.add_time=n.add_time and  a.lang_code=n.lang_code and a.country=n.country
left join
(select collect_uv,add_time,lang_code,country from  dw_proj.rosegal_pc_report_collect_uv_tmp where platform='pc' and recommend_position='mr_T_5' ) o
on a.add_time=o.add_time and  a.lang_code=o.lang_code and a.country=o.country
left join
(select collect_num,add_time,lang_code,country from  dw_proj.rosegal_pc_report_collect_num_tmp where platform='pc' and recommend_position='mr_T_5' ) p
on a.add_time=p.add_time and  a.lang_code=p.lang_code and a.country=p.country

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
  h.cart_uv / d.sku_uv * 100,
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
  'pc',
  '无搜索结果页T_6',
  '无搜索结果页-may_be_you_like',
  a.lang_code,
  a.country
from  
(select pv,add_time,lang_code,country from  dw_proj.rosegal_pc_report_pv_tmp where platform='pc' and recommend_position='may_be_you_like' ) a 
left join 
(select uv,add_time,lang_code,country from  dw_proj.rosegal_pc_report_uv_tmp where platform='pc' and recommend_position='may_be_you_like' ) b
on a.add_time=b.add_time and  a.lang_code=b.lang_code and a.country=b.country
left join
(select exp_num,add_time,lang_code,country from  dw_proj.rosegal_pc_report_exp_num_tmp where platform='pc' and recommend_position='T_6' ) c
on a.add_time=c.add_time and  a.lang_code=c.lang_code and a.country=c.country
left join
(select sku_uv,add_time,lang_code,country from  dw_proj.rosegal_pc_report_sku_uv_tmp where platform='pc' and recommend_position='T_6' ) d
on a.add_time=d.add_time and  a.lang_code=d.lang_code and a.country=d.country
left join
(select click_num,add_time,lang_code,country from  dw_proj.rosegal_pc_report_click_num_tmp where platform='pc' and recommend_position='T_6' ) e
on a.add_time=e.add_time and  a.lang_code=e.lang_code and a.country=e.country
left join
(select click_uv,add_time,lang_code,country from  dw_proj.rosegal_pc_report_click_uv_tmp where platform='pc' and recommend_position='T_6' ) f
on a.add_time=f.add_time and  a.lang_code=f.lang_code and a.country=f.country
left join
(select cart_num,add_time,lang_code,country from  dw_proj.rosegal_pc_report_cart_num_tmp where platform='pc' and recommend_position='mr_T_6' ) g
on a.add_time=g.add_time and  a.lang_code=g.lang_code and a.country=g.country
left join
(select cart_uv,add_time,lang_code,country from  dw_proj.rosegal_pc_report_cart_uv_tmp where platform='pc' and recommend_position='mr_T_6' ) h
on a.add_time=h.add_time and  a.lang_code=h.lang_code and a.country=h.country
left join
(select order_sku_num,add_time,lang_code,country from  dw_proj.rosegal_pc_report_order_sku_num_tmp where platform='pc' and recommend_position='mr_T_6' ) i
on a.add_time=i.add_time and  a.lang_code=i.lang_code and a.country=i.country
left join
(select order_uv,add_time,lang_code,country from  dw_proj.rosegal_pc_report_order_uv_tmp where platform='pc' and recommend_position='mr_T_6' ) j
on a.add_time=j.add_time and  a.lang_code=j.lang_code and a.country=j.country
left join
(select gmv,add_time,lang_code,country from  dw_proj.rosegal_pc_report_gmv_tmp where platform='pc' and recommend_position='mr_T_6' ) k
on a.add_time=k.add_time and  a.lang_code=k.lang_code and a.country=k.country
left join
(select purchase_num,add_time,lang_code,country from  dw_proj.rosegal_pc_report_purchase_num_tmp where platform='pc' and recommend_position='mr_T_6' ) l
on a.add_time=l.add_time and  a.lang_code=l.lang_code and a.country=l.country
left join
(select pay_uv,add_time,lang_code,country from  dw_proj.rosegal_pc_report_pay_uv_tmp where platform='pc' and recommend_position='mr_T_6' ) m
on a.add_time=m.add_time and  a.lang_code=m.lang_code and a.country=m.country
left join
(select pay_amount,add_time,lang_code,country from  dw_proj.rosegal_pc_report_pay_amount_tmp where platform='pc' and recommend_position='mr_T_6' ) n
on a.add_time=n.add_time and  a.lang_code=n.lang_code and a.country=n.country
left join
(select collect_uv,add_time,lang_code,country from  dw_proj.rosegal_pc_report_collect_uv_tmp where platform='pc' and recommend_position='mr_T_6' ) o
on a.add_time=o.add_time and  a.lang_code=o.lang_code and a.country=o.country
left join
(select collect_num,add_time,lang_code,country from  dw_proj.rosegal_pc_report_collect_num_tmp where platform='pc' and recommend_position='mr_T_6' ) p
on a.add_time=p.add_time and  a.lang_code=p.lang_code and a.country=p.country

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
  h.cart_uv / d.sku_uv * 100,
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
  'm',
  '商详页推荐T_2',
  '商详页推荐-related_recommendations',
  a.lang_code,
  a.country
from  
(select pv,add_time,lang_code,country from  dw_proj.rosegal_pc_report_pv_tmp where platform='m' and recommend_position='related_recommendations' ) a 
left join 
(select uv,add_time,lang_code,country from  dw_proj.rosegal_pc_report_uv_tmp where platform='m' and recommend_position='related_recommendations' ) b
on a.add_time=b.add_time and  a.lang_code=b.lang_code and a.country=b.country
left join
(select exp_num,add_time,lang_code,country from  dw_proj.rosegal_pc_report_exp_num_tmp where platform='m' and recommend_position='T_2' ) c
on a.add_time=c.add_time and  a.lang_code=c.lang_code and a.country=c.country
left join
(select sku_uv,add_time,lang_code,country from  dw_proj.rosegal_pc_report_sku_uv_tmp where platform='m' and recommend_position='T_2' ) d
on a.add_time=d.add_time and  a.lang_code=d.lang_code and a.country=d.country
left join
(select click_num,add_time,lang_code,country from  dw_proj.rosegal_pc_report_click_num_tmp where platform='m' and recommend_position='T_2' ) e
on a.add_time=e.add_time and  a.lang_code=e.lang_code and a.country=e.country
left join
(select click_uv,add_time,lang_code,country from  dw_proj.rosegal_pc_report_click_uv_tmp where platform='m' and recommend_position='T_2' ) f
on a.add_time=f.add_time and  a.lang_code=f.lang_code and a.country=f.country
left join
(select cart_num,add_time,lang_code,country from  dw_proj.rosegal_pc_report_cart_num_tmp where platform='m' and recommend_position='mr_T_2' ) g
on a.add_time=g.add_time and  a.lang_code=g.lang_code and a.country=g.country
left join
(select cart_uv,add_time,lang_code,country from  dw_proj.rosegal_pc_report_cart_uv_tmp where platform='m' and recommend_position='mr_T_2' ) h
on a.add_time=h.add_time and  a.lang_code=h.lang_code and a.country=h.country
left join
(select order_sku_num,add_time,lang_code,country from  dw_proj.rosegal_pc_report_order_sku_num_tmp where platform='m' and recommend_position='mr_T_2' ) i
on a.add_time=i.add_time and  a.lang_code=i.lang_code and a.country=i.country
left join
(select order_uv,add_time,lang_code,country from  dw_proj.rosegal_pc_report_order_uv_tmp where platform='m' and recommend_position='mr_T_2' ) j
on a.add_time=j.add_time and  a.lang_code=j.lang_code and a.country=j.country
left join
(select gmv,add_time,lang_code,country from  dw_proj.rosegal_pc_report_gmv_tmp where platform='m' and recommend_position='mr_T_2' ) k
on a.add_time=k.add_time and  a.lang_code=k.lang_code and a.country=k.country
left join
(select purchase_num,add_time,lang_code,country from  dw_proj.rosegal_pc_report_purchase_num_tmp where platform='m' and recommend_position='mr_T_2' ) l
on a.add_time=l.add_time and  a.lang_code=l.lang_code and a.country=l.country
left join
(select pay_uv,add_time,lang_code,country from  dw_proj.rosegal_pc_report_pay_uv_tmp where platform='m' and recommend_position='mr_T_2' ) m
on a.add_time=m.add_time and  a.lang_code=m.lang_code and a.country=m.country
left join
(select pay_amount,add_time,lang_code,country from  dw_proj.rosegal_pc_report_pay_amount_tmp where platform='m' and recommend_position='mr_T_2' ) n
on a.add_time=n.add_time and  a.lang_code=n.lang_code and a.country=n.country
left join
(select collect_uv,add_time,lang_code,country from  dw_proj.rosegal_pc_report_collect_uv_tmp where platform='m' and recommend_position='mr_T_2' ) o
on a.add_time=o.add_time and  a.lang_code=o.lang_code and a.country=o.country
left join
(select collect_num,add_time,lang_code,country from  dw_proj.rosegal_pc_report_collect_num_tmp where platform='m' and recommend_position='mr_T_2' ) p
on a.add_time=p.add_time and  a.lang_code=p.lang_code and a.country=p.country

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
  h.cart_uv / d.sku_uv * 100,
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
  'm',
  '空购物车页T_3',
  '空购物车页-featured_recommendations',
  a.lang_code,
  a.country
from  
(select pv,add_time,lang_code,country from  dw_proj.rosegal_pc_report_pv_tmp where platform='m' and recommend_position='featured_recommendations' ) a 
left join 
(select uv,add_time,lang_code,country from  dw_proj.rosegal_pc_report_uv_tmp where platform='m' and recommend_position='featured_recommendations' ) b
on a.add_time=b.add_time and  a.lang_code=b.lang_code and a.country=b.country
left join
(select exp_num,add_time,lang_code,country from  dw_proj.rosegal_pc_report_exp_num_tmp where platform='m' and recommend_position='T_3' ) c
on a.add_time=c.add_time and  a.lang_code=c.lang_code and a.country=c.country
left join
(select sku_uv,add_time,lang_code,country from  dw_proj.rosegal_pc_report_sku_uv_tmp where platform='m' and recommend_position='T_3' ) d
on a.add_time=d.add_time and  a.lang_code=d.lang_code and a.country=d.country
left join
(select click_num,add_time,lang_code,country from  dw_proj.rosegal_pc_report_click_num_tmp where platform='m' and recommend_position='T_3' ) e
on a.add_time=e.add_time and  a.lang_code=e.lang_code and a.country=e.country
left join
(select click_uv,add_time,lang_code,country from  dw_proj.rosegal_pc_report_click_uv_tmp where platform='m' and recommend_position='T_3' ) f
on a.add_time=f.add_time and  a.lang_code=f.lang_code and a.country=f.country
left join
(select cart_num,add_time,lang_code,country from  dw_proj.rosegal_pc_report_cart_num_tmp where platform='m' and recommend_position='mr_T_3' ) g
on a.add_time=g.add_time and  a.lang_code=g.lang_code and a.country=g.country
left join
(select cart_uv,add_time,lang_code,country from  dw_proj.rosegal_pc_report_cart_uv_tmp where platform='m' and recommend_position='mr_T_3' ) h
on a.add_time=h.add_time and  a.lang_code=h.lang_code and a.country=h.country
left join
(select order_sku_num,add_time,lang_code,country from  dw_proj.rosegal_pc_report_order_sku_num_tmp where platform='m' and recommend_position='mr_T_3' ) i
on a.add_time=i.add_time and  a.lang_code=i.lang_code and a.country=i.country
left join
(select order_uv,add_time,lang_code,country from  dw_proj.rosegal_pc_report_order_uv_tmp where platform='m' and recommend_position='mr_T_3' ) j
on a.add_time=j.add_time and  a.lang_code=j.lang_code and a.country=j.country
left join
(select gmv,add_time,lang_code,country from  dw_proj.rosegal_pc_report_gmv_tmp where platform='m' and recommend_position='mr_T_3' ) k
on a.add_time=k.add_time and  a.lang_code=k.lang_code and a.country=k.country
left join
(select purchase_num,add_time,lang_code,country from  dw_proj.rosegal_pc_report_purchase_num_tmp where platform='m' and recommend_position='mr_T_3' ) l
on a.add_time=l.add_time and  a.lang_code=l.lang_code and a.country=l.country
left join
(select pay_uv,add_time,lang_code,country from  dw_proj.rosegal_pc_report_pay_uv_tmp where platform='m' and recommend_position='mr_T_3' ) m
on a.add_time=m.add_time and  a.lang_code=m.lang_code and a.country=m.country
left join
(select pay_amount,add_time,lang_code,country from  dw_proj.rosegal_pc_report_pay_amount_tmp where platform='m' and recommend_position='mr_T_3' ) n
on a.add_time=n.add_time and  a.lang_code=n.lang_code and a.country=n.country
left join
(select collect_uv,add_time,lang_code,country from  dw_proj.rosegal_pc_report_collect_uv_tmp where platform='m' and recommend_position='mr_T_3' ) o
on a.add_time=o.add_time and  a.lang_code=o.lang_code and a.country=o.country
left join
(select collect_num,add_time,lang_code,country from  dw_proj.rosegal_pc_report_collect_num_tmp where platform='m' and recommend_position='mr_T_3' ) p
on a.add_time=p.add_time and  a.lang_code=p.lang_code and a.country=p.country

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
  h.cart_uv / d.sku_uv * 100,
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
  'm',
  '无搜索结果页T_4',
  '无搜索结果页-may_be_you_like',
  a.lang_code,
  a.country
from  
(select pv,add_time,lang_code,country from  dw_proj.rosegal_pc_report_pv_tmp where platform='m' and recommend_position='related_recommendations' ) a 
left join 
(select uv,add_time,lang_code,country from  dw_proj.rosegal_pc_report_uv_tmp where platform='m' and recommend_position='related_recommendations' ) b
on a.add_time=b.add_time and  a.lang_code=b.lang_code and a.country=b.country
left join
(select exp_num,add_time,lang_code,country from  dw_proj.rosegal_pc_report_exp_num_tmp where platform='m' and recommend_position='T_4' ) c
on a.add_time=c.add_time and  a.lang_code=c.lang_code and a.country=c.country
left join
(select sku_uv,add_time,lang_code,country from  dw_proj.rosegal_pc_report_sku_uv_tmp where platform='m' and recommend_position='T_4' ) d
on a.add_time=d.add_time and  a.lang_code=d.lang_code and a.country=d.country
left join
(select click_num,add_time,lang_code,country from  dw_proj.rosegal_pc_report_click_num_tmp where platform='m' and recommend_position='T_4' ) e
on a.add_time=e.add_time and  a.lang_code=e.lang_code and a.country=e.country
left join
(select click_uv,add_time,lang_code,country from  dw_proj.rosegal_pc_report_click_uv_tmp where platform='m' and recommend_position='T_4' ) f
on a.add_time=f.add_time and  a.lang_code=f.lang_code and a.country=f.country
left join
(select cart_num,add_time,lang_code,country from  dw_proj.rosegal_pc_report_cart_num_tmp where platform='m' and recommend_position='mr_T_4' ) g
on a.add_time=g.add_time and  a.lang_code=g.lang_code and a.country=g.country
left join
(select cart_uv,add_time,lang_code,country from  dw_proj.rosegal_pc_report_cart_uv_tmp where platform='m' and recommend_position='mr_T_4' ) h
on a.add_time=h.add_time and  a.lang_code=h.lang_code and a.country=h.country
left join
(select order_sku_num,add_time,lang_code,country from  dw_proj.rosegal_pc_report_order_sku_num_tmp where platform='m' and recommend_position='mr_T_4' ) i
on a.add_time=i.add_time and  a.lang_code=i.lang_code and a.country=i.country
left join
(select order_uv,add_time,lang_code,country from  dw_proj.rosegal_pc_report_order_uv_tmp where platform='m' and recommend_position='mr_T_4' ) j
on a.add_time=j.add_time and  a.lang_code=j.lang_code and a.country=j.country
left join
(select gmv,add_time,lang_code,country from  dw_proj.rosegal_pc_report_gmv_tmp where platform='m' and recommend_position='mr_T_4' ) k
on a.add_time=k.add_time and  a.lang_code=k.lang_code and a.country=k.country
left join
(select purchase_num,add_time,lang_code,country from  dw_proj.rosegal_pc_report_purchase_num_tmp where platform='m' and recommend_position='mr_T_4' ) l
on a.add_time=l.add_time and  a.lang_code=l.lang_code and a.country=l.country
left join
(select pay_uv,add_time,lang_code,country from  dw_proj.rosegal_pc_report_pay_uv_tmp where platform='m' and recommend_position='mr_T_4' ) m
on a.add_time=m.add_time and  a.lang_code=m.lang_code and a.country=m.country
left join
(select pay_amount,add_time,lang_code,country from  dw_proj.rosegal_pc_report_pay_amount_tmp where platform='m' and recommend_position='mr_T_4' ) n
on a.add_time=n.add_time and  a.lang_code=n.lang_code and a.country=n.country
left join
(select collect_uv,add_time,lang_code,country from  dw_proj.rosegal_pc_report_collect_uv_tmp where platform='m' and recommend_position='mr_T_4' ) o
on a.add_time=o.add_time and  a.lang_code=o.lang_code and a.country=o.country
left join
(select collect_num,add_time,lang_code,country from  dw_proj.rosegal_pc_report_collect_num_tmp where platform='m' and recommend_position='mr_T_4' ) p
on a.add_time=p.add_time and  a.lang_code=p.lang_code and a.country=p.country
;



--结果导出到mysql
INSERT OVERWRITE TABLE dw_proj.rosegal_pc_recommend_position_report_exp
select
 *
from 
  dw_proj.rosegal_pc_recommend_position_report
where 
  add_time = '${ADD_TIME}'
  and platform = 'm' 
-- union all 
-- select
--  *
-- from 
--   dw_proj.rosegal_pc_recommend_position_report
-- where 
--   add_time = '${ADD_TIME}'
--   and platform = 'pc' 
--   and recommend_position ='空购物车页-featured_recommendations'
;