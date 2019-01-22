--@author ZhanRui
--@date 2018年7月24日 
--@desc  Zaful PC/M推荐位报表

SET mapred.job.name=zaful_recommend_position_report;
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
CREATE TABLE IF NOT EXISTS dw_zaful_recommend.zaful_recommend_position_report(
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
position_name             STRING         COMMENT "推荐位名称",
glb_dc                    STRING         COMMENT "语言站",
country                   STRING         COMMENT "国家"
)
COMMENT '推荐位数据报表'
PARTITIONED BY (add_time STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\u0001' LINES TERMINATED BY '\n' STORED AS TEXTFILE
;


--页面PV 表zf_pc_event_info
--条件：glb_plf=pc,glb_t=ie,glb_b=a,glb_ubcta为空，计算glb_t的数量
CREATE TABLE IF NOT EXISTS dw_zaful_recommend.report_pv_tmp(
pv                        INT            COMMENT "页面PV",
glb_plf                   STRING         COMMENT "平台",
glb_b                     STRING         COMMENT "页面大类",
country                   STRING         COMMENT "国家",
glb_dc                    STRING         COMMENT "语言站",
add_time                  STRING         COMMENT "日期"
)
COMMENT "推荐位报表页面PV中间表"
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\u0001' LINES TERMINATED BY '\n' STORED AS TEXTFILE;

--页面UV	表zf_pc_event_info
--条件：glb_plf=pc,glb_b=a，计算glb_od去重后的数量
CREATE TABLE IF NOT EXISTS dw_zaful_recommend.report_uv_tmp(
uv                        INT            COMMENT "页面UV",
glb_plf                   STRING         COMMENT "平台",
glb_b                     STRING         COMMENT "页面大类",
country                   STRING         COMMENT "国家",
glb_dc                    STRING         COMMENT "语言站",
add_time                  STRING         COMMENT "日期"
)
COMMENT "推荐位报表页面UV中间表"
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\u0001' LINES TERMINATED BY '\n' STORED AS TEXTFILE;

--商品曝光数	表zf_pc_event_info
--条件：glb_b=a,glb_t=ie,glb_pm=mr,glb_plf=pc，glb_ubcta中jason字段mdlc=T_1，计算glb_ubcta中jason字段sku的数量
CREATE TABLE IF NOT EXISTS dw_zaful_recommend.report_exp_tmp(
exp_num                   INT            COMMENT "商品曝光数",
glb_plf                   STRING         COMMENT "平台",
glb_b                     STRING         COMMENT "页面大类",
mdlc                      STRING         COMMENT "推荐位编号",
country                   STRING         COMMENT "国家",
glb_dc                    STRING         COMMENT "语言站",
add_time                  STRING         COMMENT "日期"
)
COMMENT "推荐位报表商品曝光数中间表"
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\u0001' LINES TERMINATED BY '\n' STORED AS TEXTFILE;

--商品点击数	表zf_pc_event_info
--条件：glb_b=a,glb_t=ic,glb_pm=mr,glb_plf=pc，glb_x=sku，glb_ubcta中jason字段mdlc=T_1,计算glb_ubcta中jason字段sku的数量
CREATE TABLE IF NOT EXISTS dw_zaful_recommend.report_click_tmp(
click_num                 INT            COMMENT "商品点击数",
glb_plf                   STRING         COMMENT "平台",
glb_b                     STRING         COMMENT "页面大类",
mdlc                      STRING         COMMENT "推荐位编号",
country                   STRING         COMMENT "国家",
glb_dc                    STRING         COMMENT "语言站",
add_time                  STRING         COMMENT "日期"
)
COMMENT "推荐位报表商品点击数中间表"
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\u0001' LINES TERMINATED BY '\n' STORED AS TEXTFILE;

--商品加购数	表zf_pc_event_info
--条件：glb_t=ic,glb_x=ADT,glb_ubcta中jason字段fmd=mr_T_1,glb_plf=pc，计算glb_t的数量
CREATE TABLE IF NOT EXISTS dw_zaful_recommend.report_cart_tmp(
cart_num                  INT            COMMENT "商品加购数",
glb_plf                   STRING         COMMENT "平台",
fmd                       STRING         COMMENT "推荐位编号",
country                   STRING         COMMENT "国家",
glb_dc                    STRING         COMMENT "语言站",
add_time                  STRING         COMMENT "日期"
)
COMMENT "推荐位报表商品加购数中间表"
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\u0001' LINES TERMINATED BY '\n' STORED AS TEXTFILE;

--商品下单数	表zf_pc_event_info、zaful_eload_order_info、zaful_eload_order_goods

--中间表report_sku_user_tmp：取sku,user_id
CREATE TABLE IF NOT EXISTS dw_zaful_recommend.report_sku_user_country_tmp(
sku                       STRING         COMMENT "sku",
user_id                   STRING         COMMENT "用户ID",
glb_plf                   STRING         COMMENT "平台",
country                   STRING         COMMENT "国家",
glb_dc                    STRING         COMMENT "语言站",
fmd                       STRING         COMMENT "推荐位编号"
)
COMMENT "推荐位报表user-sku-county中间表"
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\u0001' LINES TERMINATED BY '\n' STORED AS TEXTFILE;

CREATE TABLE IF NOT EXISTS dw_zaful_recommend.report_sku_user_tmp(
goods_sn                  STRING         COMMENT "sku",
user_id                   STRING         COMMENT "用户ID",
order_status              STRING         COMMENT "订单状态",
pay_amount                decimal(10,2)  COMMENT "购买金额",
goods_number              STRING         COMMENT "商品数量"
)
COMMENT "推荐位报表user-sku中间表"
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\u0001' LINES TERMINATED BY '\n' STORED AS TEXTFILE;

--商品下单数结果表
CREATE TABLE IF NOT EXISTS dw_zaful_recommend.report_order_tmp(
order_num                 INT            COMMENT "商品下单数",
glb_plf                   STRING         COMMENT "平台",
fmd                       STRING         COMMENT "推荐位编号",
country                   STRING         COMMENT "国家",
glb_dc                    STRING         COMMENT "语言站",
add_time                  STRING         COMMENT "日期"
)
COMMENT "推荐位报表商品下单数中间表"
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\u0001' LINES TERMINATED BY '\n' STORED AS TEXTFILE;

--支付订单数	表zf_pc_event_info、zaful_eload_order_info、zaful_eload_order_goods
CREATE TABLE IF NOT EXISTS dw_zaful_recommend.report_purchase_tmp(
purchase_num              INT            COMMENT "支付订单数",
glb_plf                   STRING         COMMENT "平台",
fmd                       STRING         COMMENT "推荐位编号",
country                   STRING         COMMENT "国家",
glb_dc                    STRING         COMMENT "语言站",
add_time                  STRING         COMMENT "日期"
)
COMMENT "推荐位报表支付订单数中间表"
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\u0001' LINES TERMINATED BY '\n' STORED AS TEXTFILE;

--购买金额	表zf_pc_event_info、zaful_eload_order_info、zaful_eload_order_goods
CREATE TABLE IF NOT EXISTS dw_zaful_recommend.report_pay_amount_tmp(
pay_amount                decimal(10,2)  COMMENT "购买金额",
glb_plf                   STRING         COMMENT "平台",
fmd                       STRING         COMMENT "推荐位编号",
country                   STRING         COMMENT "国家",
glb_dc                    STRING         COMMENT "语言站",
add_time                  STRING         COMMENT "日期"
)
COMMENT "推荐位报表购买金额中间表"
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\u0001' LINES TERMINATED BY '\n' STORED AS TEXTFILE;

--商品收藏数	表zf_pc_event_info
CREATE TABLE IF NOT EXISTS dw_zaful_recommend.report_collect_tmp(
collect_num               INT            COMMENT "商品收藏数",
glb_plf                   STRING         COMMENT "平台",
fmd                       STRING         COMMENT "推荐位编号",
country                   STRING         COMMENT "国家",
glb_dc                    STRING         COMMENT "语言站",
add_time                  STRING         COMMENT "日期"
)
COMMENT "推荐位报表商品收藏数中间表"
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\u0001' LINES TERMINATED BY '\n' STORED AS TEXTFILE;



--cookie user 对应关系表每日更新埋点信息，汇总全量。此表先跑，其他任务可能用到。
INSERT OVERWRITE TABLE dw_zaful_recommend.zaful_od_u_map
SELECT
  m.glb_od,
  m.glb_u
from
  (
    SELECT
      glb_od,
      glb_u
    FROM
      dw_zaful_recommend.zaful_od_u_map
    union all
    SELECT
      glb_od,
      glb_u
    FROM
      stg.zf_pc_event_info
    WHERE
      concat_ws('-', year, month, day) = '${ADD_TIME}'
      AND glb_u rlike '^[0-9]+$'
  ) m
GROUP BY
  m.glb_od,
  m.glb_u
;

--中间表report_sku_user_tmp：取sku,user_id
INSERT OVERWRITE TABLE dw_zaful_recommend.report_sku_user_country_tmp
SELECT
  m.sku,
  n.glb_u,
  m.glb_plf,
  m.geoip_country_name,
  m.glb_dc,
  m.fmd
FROM
  (
    SELECT
      glb_od,
      regexp_extract(glb_skuinfo, '(.*?sku":")([0-9a-zA-Z]*)(".*?)', 2) AS sku,
      glb_plf,
      geoip_country_name,
      glb_dc,
      get_json_object(glb_ubcta, '$.fmd') AS fmd
    FROM
      stg.zf_pc_event_info
    WHERE
      concat_ws('-', year, month, day) BETWEEN '${ADD_TIME_W}'
      AND '${ADD_TIME}'
      AND glb_t = 'ic'
      AND glb_x = 'ADT'
      AND glb_skuinfo <> ''
      AND glb_ubcta <> ''
  ) m
  INNER JOIN dw_zaful_recommend.zaful_od_u_map n ON m.glb_od = n.glb_od
GROUP BY
  m.glb_plf,
  m.fmd,
  m.sku,
  n.glb_u,
  m.geoip_country_name,
  m.glb_dc;

--中间表report_sku_user_tmp：取sku,user_id
INSERT OVERWRITE TABLE dw_zaful_recommend.report_sku_user_tmp
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
      from_unixtime(add_time, 'yyyy-MM-dd') = '${ADD_TIME}'
  ) x
  JOIN   (
  SELECT goods_sn,order_id,
  goods_number,
  case when goods_pay_amount <> '0' then goods_pay_amount
  else goods_price*goods_number end as pay_amount
  from
   stg.zaful_eload_order_goods ) p
  ON x.order_id = p.order_id
group by
  p.goods_sn,
  x.user_id,
  x.order_status,
  p.pay_amount,
  p.goods_number
;


--页面PV
INSERT OVERWRITE TABLE dw_zaful_recommend.report_pv_tmp
SELECT
  count(*) as pv,
  glb_plf,
  glb_b,
  geoip_country_name,
  glb_dc,
  '${ADD_TIME}' as add_time
FROM
  stg.zf_pc_event_info
WHERE
  year = '${YEAR}'
  AND month = '${MONTH}'
  AND day = '${DAY}'
  and glb_t = 'ie'
  and glb_ubcta = ''
group by
  glb_plf,
  glb_b,
  geoip_country_name,
  glb_dc;

--页面UV
INSERT OVERWRITE TABLE dw_zaful_recommend.report_uv_tmp
SELECT
  count(*) AS uv,
  m.glb_plf,
  m.glb_b,
  m.geoip_country_name,
  m.glb_dc,
  '${ADD_TIME}' as add_time
FROM
  (
    SELECT
      glb_od,
      glb_plf,
      glb_b,
      geoip_country_name,
      glb_dc
    FROM
      stg.zf_pc_event_info
    WHERE
      year = '${YEAR}'
      AND month = '${MONTH}'
      AND day = '${DAY}'
    GROUP BY
      glb_od,
      glb_plf,
      glb_b,
      geoip_country_name,
      glb_dc
  ) m
group by
  m.glb_plf,
  m.glb_b,
  m.geoip_country_name,
  m.glb_dc;


--商品曝光数
INSERT OVERWRITE TABLE dw_zaful_recommend.report_exp_tmp
SELECT
  count(*) AS exp_num,
  n.glb_plf,
  n.glb_b,
  m.mrlc,
  n.geoip_country_name,
  n.glb_dc,
  '${ADD_TIME}' as add_time
FROM
  (
    SELECT
      log_id,
      get_json_object(glb_ubcta_col, '$.mrlc') as mrlc
    FROM
      stg.zf_pc_event_ubcta_info
    WHERE
      year = '${YEAR}'
      AND month = '${MONTH}'
      AND day = '${DAY}'
      AND get_json_object(glb_ubcta_col, '$.sku') <> ''
  ) m
  INNER JOIN (
    SELECT
      log_id,
      glb_plf,
      glb_b,
      geoip_country_name,
      glb_dc
    FROM
      stg.zf_pc_event_info
    WHERE
      year = '${YEAR}'
      AND month = '${MONTH}'
      AND day = '${DAY}'
      AND glb_t = 'ie'
      AND glb_pm = 'mr'
      AND glb_ubcta <> ''
  ) n ON m.log_id = n.log_id
group by
  n.glb_plf,
  n.glb_b,
  m.mrlc,
  n.geoip_country_name,
  n.glb_dc;

--商品点击数
INSERT OVERWRITE TABLE dw_zaful_recommend.report_click_tmp
SELECT
  count(*) AS click_num,
  glb_plf,
  glb_b,
  get_json_object(glb_ubcta, '$.mrlc') as mrlc,
  geoip_country_name,
  glb_dc,
  '${ADD_TIME}' as add_time
FROM
  stg.zf_pc_event_info
WHERE
  year = '${YEAR}'
  AND month = '${MONTH}'
  AND day = '${DAY}'
  AND glb_t = 'ic'
  AND glb_pm = 'mr'
  AND glb_x = 'sku'
  AND glb_skuinfo <> ''
  AND glb_ubcta <> ''
group by
  glb_plf,
  glb_b,
  get_json_object(glb_ubcta, '$.mrlc'),
  geoip_country_name,
  glb_dc;


--商品加购数
INSERT OVERWRITE TABLE dw_zaful_recommend.report_cart_tmp
SELECT
  count(*) AS cart_num,
  glb_plf,
  get_json_object(glb_ubcta, '$.fmd') as fmd,
  geoip_country_name,
  glb_dc,
  '${ADD_TIME}' as add_time
FROM
  stg.zf_pc_event_info
WHERE
  year = '${YEAR}'
  AND month = '${MONTH}'
  AND day = '${DAY}'
  AND glb_t = 'ic'
  AND glb_x = 'ADT'
group by
  glb_plf,
  get_json_object(glb_ubcta, '$.fmd'),
  geoip_country_name,
  glb_dc
;



--下单商品数
INSERT OVERWRITE TABLE dw_zaful_recommend.report_order_tmp
SELECT
  SUM(x2.goods_number) AS order_num,
  x1.glb_plf,
  x1.fmd,
  x1.country,
  x1.glb_dc,
  '${ADD_TIME}' as add_time
from
  dw_zaful_recommend.report_sku_user_country_tmp x1
  INNER JOIN dw_zaful_recommend.report_sku_user_tmp x2 ON x1.user_id = x2.user_id
  AND x1.sku = x2.goods_sn
group by
  x1.glb_plf,
  x1.fmd,
  x1.country,
  x1.glb_dc;

--销量
INSERT OVERWRITE TABLE dw_zaful_recommend.report_purchase_tmp
SELECT
  SUM(x2.goods_number) AS purchase_num,
  x1.glb_plf,
  x1.fmd,
  x1.country,
  x1.glb_dc,
  '${ADD_TIME}' as add_time
from
  dw_zaful_recommend.report_sku_user_country_tmp x1
  INNER JOIN dw_zaful_recommend.report_sku_user_tmp x2 ON x1.user_id = x2.user_id
  AND x1.sku = x2.goods_sn
where
  x2.order_status not in ('0', '11')
group by
  x1.glb_plf,
  x1.fmd,
  x1.country,
  x1.glb_dc;


--销售额
INSERT OVERWRITE TABLE dw_zaful_recommend.report_pay_amount_tmp
SELECT
  sum(x2.pay_amount) as pay_amount,
  x1.glb_plf,
  x1.fmd,
  x1.country,
  x1.glb_dc,
  '${ADD_TIME}' as add_time
from
  dw_zaful_recommend.report_sku_user_country_tmp x1
  INNER JOIN dw_zaful_recommend.report_sku_user_tmp x2 ON x1.user_id = x2.user_id
  AND x1.sku = x2.goods_sn
where
  x2.order_status not in ('0', '11')
group by
  x1.glb_plf,
  x1.fmd,
  x1.country,
  x1.glb_dc;

--商品收藏数	
INSERT OVERWRITE TABLE dw_zaful_recommend.report_collect_tmp
SELECT
  count(*) as collect_num,
  glb_plf,
  get_json_object(glb_ubcta, '$.fmd') AS fmd,
  geoip_country_name,
  glb_dc,
  '${ADD_TIME}' as add_time
FROM
  stg.zf_pc_event_info
WHERE
  year = '${YEAR}'
  AND month = '${MONTH}'
  AND day = '${DAY}'
  AND glb_t = 'ic'
  AND glb_x = 'ADF'
  AND glb_u <> ''
group by
  glb_plf,
  get_json_object(glb_ubcta, '$.fmd'),
  geoip_country_name,
  glb_dc;




--所有结果汇总
INSERT overwrite TABLE dw_zaful_recommend.zaful_recommend_position_report PARTITION (add_time = '${ADD_TIME}')
select
  a.pv,
  b.uv,
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
  'PC',
  'T_1',
  '首页-New arrival',
  a.glb_dc,
  a.country
from
  (
    select
      pv,
      add_time,
      glb_dc,
      country
    from
      dw_zaful_recommend.report_pv_tmp
    where
      glb_plf = 'pc'
      and glb_b = 'a'
  ) a
  left join (
    select
      uv,
      add_time,
      glb_dc,
      country
    from
      dw_zaful_recommend.report_uv_tmp
    where
      glb_plf = 'pc'
      and glb_b = 'a'
  ) b on a.add_time = b.add_time
  and a.glb_dc = b.glb_dc
  and a.country = b.country
  left join (
    select
      exp_num,
      add_time,
      glb_dc,
      country
    from
      dw_zaful_recommend.report_exp_tmp
    where
      glb_plf = 'pc'
      and glb_b = 'a'
      and mdlc = 'T_1'
  ) c on a.add_time = c.add_time
  and a.glb_dc = c.glb_dc
  and a.country = c.country
  left join (
    select
      click_num,
      add_time,
      glb_dc,
      country
    from
      dw_zaful_recommend.report_click_tmp
    where
      glb_plf = 'pc'
      and glb_b = 'a'
      and mdlc = 'T_1'
  ) d on a.add_time = d.add_time
  and a.glb_dc = d.glb_dc
  and a.country = d.country
  left join (
    select
      cart_num,
      add_time,
      glb_dc,
      country
    from
      dw_zaful_recommend.report_cart_tmp
    where
      glb_plf = 'pc'
      and fmd = 'mr_T_1'
  ) e on a.add_time = e.add_time
  and a.glb_dc = e.glb_dc
  and a.country = e.country
  left join (
    select
      order_num,
      add_time,
      glb_dc,
      country
    from
      dw_zaful_recommend.report_order_tmp
    where
      glb_plf = 'pc'
      and fmd = 'mr_T_1'
  ) f on a.add_time = f.add_time
  and a.glb_dc = f.glb_dc
  and a.country = f.country
  left join (
    select
      purchase_num,
      add_time,
      glb_dc,
      country
    from
      dw_zaful_recommend.report_purchase_tmp
    where
      glb_plf = 'pc'
      and fmd = 'mr_T_1'
  ) g on a.add_time = g.add_time
  and a.glb_dc = g.glb_dc
  and a.country = g.country
  left join (
    select
      pay_amount,
      add_time,
      glb_dc,
      country
    from
      dw_zaful_recommend.report_pay_amount_tmp
    where
      glb_plf = 'pc'
      and fmd = 'mr_T_1'
  ) h on a.add_time = h.add_time
  and a.glb_dc = h.glb_dc
  and a.country = h.country
  left join (
    select
      collect_num,
      add_time,
      glb_dc,
      country
    from
      dw_zaful_recommend.report_collect_tmp
    where
      glb_plf = 'pc'
      and fmd = 'mr_T_1'
  ) i on a.add_time = i.add_time
  and a.glb_dc = i.glb_dc
  and a.country = i.country
union all
select
  a.pv,
  b.uv,
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
  'PC',
  'T_2',
  '首页-Recommend for you',
  a.glb_dc,
  a.country
from
  (
    select
      pv,
      add_time,
      glb_dc,
      country
    from
      dw_zaful_recommend.report_pv_tmp
    where
      glb_plf = 'pc'
      and glb_b = 'a'
  ) a
  left join (
    select
      uv,
      add_time,
      glb_dc,
      country
    from
      dw_zaful_recommend.report_uv_tmp
    where
      glb_plf = 'pc'
      and glb_b = 'a'
  ) b on a.add_time = b.add_time
  and a.glb_dc = b.glb_dc
  and a.country = b.country
  left join (
    select
      exp_num,
      add_time,
      glb_dc,
      country
    from
      dw_zaful_recommend.report_exp_tmp
    where
      glb_plf = 'pc'
      and glb_b = 'a'
      and mdlc = 'T_2'
  ) c on a.add_time = c.add_time
  and a.glb_dc = c.glb_dc
  and a.country = c.country
  left join (
    select
      click_num,
      add_time,
      glb_dc,
      country
    from
      dw_zaful_recommend.report_click_tmp
    where
      glb_plf = 'pc'
      and glb_b = 'a'
      and mdlc = 'T_2'
  ) d on a.add_time = d.add_time
  and a.glb_dc = d.glb_dc
  and a.country = d.country
  left join (
    select
      cart_num,
      add_time,
      glb_dc,
      country
    from
      dw_zaful_recommend.report_cart_tmp
    where
      glb_plf = 'pc'
      and fmd = 'mr_T_2'
  ) e on a.add_time = e.add_time
  and a.glb_dc = e.glb_dc
  and a.country = e.country
  left join (
    select
      order_num,
      add_time,
      glb_dc,
      country
    from
      dw_zaful_recommend.report_order_tmp
    where
      glb_plf = 'pc'
      and fmd = 'mr_T_2'
  ) f on a.add_time = f.add_time
  and a.glb_dc = f.glb_dc
  and a.country = f.country
  left join (
    select
      purchase_num,
      add_time,
      glb_dc,
      country
    from
      dw_zaful_recommend.report_purchase_tmp
    where
      glb_plf = 'pc'
      and fmd = 'mr_T_2'
  ) g on a.add_time = g.add_time
  and a.glb_dc = g.glb_dc
  and a.country = g.country
  left join (
    select
      pay_amount,
      add_time,
      glb_dc,
      country
    from
      dw_zaful_recommend.report_pay_amount_tmp
    where
      glb_plf = 'pc'
      and fmd = 'mr_T_2'
  ) h on a.add_time = h.add_time
  and a.glb_dc = h.glb_dc
  and a.country = h.country
  left join (
    select
      collect_num,
      add_time,
      glb_dc,
      country
    from
      dw_zaful_recommend.report_collect_tmp
    where
      glb_plf = 'pc'
      and fmd = 'mr_T_2'
  ) i on a.add_time = i.add_time
  and a.glb_dc = i.glb_dc
  and a.country = i.country
union all
select
  a.pv,
  b.uv,
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
  'PC',
  'T_3',
  '商详页-YOU MIGHT ALSO LIKE',
  a.glb_dc,
  a.country
from
  (
    select
      pv,
      add_time,
      glb_dc,
      country
    from
      dw_zaful_recommend.report_pv_tmp
    where
      glb_plf = 'pc'
      and glb_b = 'c'
  ) a
  left join (
    select
      uv,
      add_time,
      glb_dc,
      country
    from
      dw_zaful_recommend.report_uv_tmp
    where
      glb_plf = 'pc'
      and glb_b = 'c'
  ) b on a.add_time = b.add_time
  and a.glb_dc = b.glb_dc
  and a.country = b.country
  left join (
    select
      exp_num,
      add_time,
      glb_dc,
      country
    from
      dw_zaful_recommend.report_exp_tmp
    where
      glb_plf = 'pc'
      and glb_b = 'c'
      and mdlc = 'T_3'
  ) c on a.add_time = c.add_time
  and a.glb_dc = c.glb_dc
  and a.country = c.country
  left join (
    select
      click_num,
      add_time,
      glb_dc,
      country
    from
      dw_zaful_recommend.report_click_tmp
    where
      glb_plf = 'pc'
      and glb_b = 'c'
      and mdlc = 'T_3'
  ) d on a.add_time = d.add_time
  and a.glb_dc = d.glb_dc
  and a.country = d.country
  left join (
    select
      cart_num,
      add_time,
      glb_dc,
      country
    from
      dw_zaful_recommend.report_cart_tmp
    where
      glb_plf = 'pc'
      and fmd = 'mr_T_3'
  ) e on a.add_time = e.add_time
  and a.glb_dc = e.glb_dc
  and a.country = e.country
  left join (
    select
      order_num,
      add_time,
      glb_dc,
      country
    from
      dw_zaful_recommend.report_order_tmp
    where
      glb_plf = 'pc'
      and fmd = 'mr_T_3'
  ) f on a.add_time = f.add_time
  and a.glb_dc = f.glb_dc
  and a.country = f.country
  left join (
    select
      purchase_num,
      add_time,
      glb_dc,
      country
    from
      dw_zaful_recommend.report_purchase_tmp
    where
      glb_plf = 'pc'
      and fmd = 'mr_T_3'
  ) g on a.add_time = g.add_time
  and a.glb_dc = g.glb_dc
  and a.country = g.country
  left join (
    select
      pay_amount,
      add_time,
      glb_dc,
      country
    from
      dw_zaful_recommend.report_pay_amount_tmp
    where
      glb_plf = 'pc'
      and fmd = 'mr_T_3'
  ) h on a.add_time = h.add_time
  and a.glb_dc = h.glb_dc
  and a.country = h.country
  left join (
    select
      collect_num,
      add_time,
      glb_dc,
      country
    from
      dw_zaful_recommend.report_collect_tmp
    where
      glb_plf = 'pc'
      and fmd = 'mr_T_3'
  ) i on a.add_time = i.add_time
  and a.glb_dc = i.glb_dc
  and a.country = i.country
union all
select
  a.pv,
  b.uv,
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
  'PC',
  'T_4',
  '商详页-YOUR RECENT HISTORY',
  a.glb_dc,
  a.country
from
  (
    select
      pv,
      add_time,
      glb_dc,
      country
    from
      dw_zaful_recommend.report_pv_tmp
    where
      glb_plf = 'pc'
      and glb_b = 'c'
  ) a
  left join (
    select
      uv,
      add_time,
      glb_dc,
      country
    from
      dw_zaful_recommend.report_uv_tmp
    where
      glb_plf = 'pc'
      and glb_b = 'c'
  ) b on a.add_time = b.add_time
  and a.glb_dc = b.glb_dc
  and a.country = b.country
  left join (
    select
      exp_num,
      add_time,
      glb_dc,
      country
    from
      dw_zaful_recommend.report_exp_tmp
    where
      glb_plf = 'pc'
      and glb_b = 'c'
      and mdlc = 'T_4'
  ) c on a.add_time = c.add_time
  and a.glb_dc = c.glb_dc
  and a.country = c.country
  left join (
    select
      click_num,
      add_time,
      glb_dc,
      country
    from
      dw_zaful_recommend.report_click_tmp
    where
      glb_plf = 'pc'
      and glb_b = 'c'
      and mdlc = 'T_4'
  ) d on a.add_time = d.add_time
  and a.glb_dc = d.glb_dc
  and a.country = d.country
  left join (
    select
      cart_num,
      add_time,
      glb_dc,
      country
    from
      dw_zaful_recommend.report_cart_tmp
    where
      glb_plf = 'pc'
      and fmd = 'mr_T_4'
  ) e on a.add_time = e.add_time
  and a.glb_dc = e.glb_dc
  and a.country = e.country
  left join (
    select
      order_num,
      add_time,
      glb_dc,
      country
    from
      dw_zaful_recommend.report_order_tmp
    where
      glb_plf = 'pc'
      and fmd = 'mr_T_4'
  ) f on a.add_time = f.add_time
  and a.glb_dc = f.glb_dc
  and a.country = f.country
  left join (
    select
      purchase_num,
      add_time,
      glb_dc,
      country
    from
      dw_zaful_recommend.report_purchase_tmp
    where
      glb_plf = 'pc'
      and fmd = 'mr_T_4'
  ) g on a.add_time = g.add_time
  and a.glb_dc = g.glb_dc
  and a.country = g.country
  left join (
    select
      pay_amount,
      add_time,
      glb_dc,
      country
    from
      dw_zaful_recommend.report_pay_amount_tmp
    where
      glb_plf = 'pc'
      and fmd = 'mr_T_4'
  ) h on a.add_time = h.add_time
  and a.glb_dc = h.glb_dc
  and a.country = h.country
  left join (
    select
      collect_num,
      add_time,
      glb_dc,
      country
    from
      dw_zaful_recommend.report_collect_tmp
    where
      glb_plf = 'pc'
      and fmd = 'mr_T_4'
  ) i on a.add_time = i.add_time
  and a.glb_dc = i.glb_dc
  and a.country = i.country
union all
select
  a.pv,
  b.uv,
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
  'PC',
  'T_9',
  '商详页-T9推荐位',
  a.glb_dc,
  a.country
from
  (
    select
      pv,
      add_time,
      glb_dc,
      country
    from
      dw_zaful_recommend.report_pv_tmp
    where
      glb_plf = 'pc'
      and glb_b = 'c'
  ) a
  left join (
    select
      uv,
      add_time,
      glb_dc,
      country
    from
      dw_zaful_recommend.report_uv_tmp
    where
      glb_plf = 'pc'
      and glb_b = 'c'
  ) b on a.add_time = b.add_time
  and a.glb_dc = b.glb_dc
  and a.country = b.country
  left join (
    select
      exp_num,
      add_time,
      glb_dc,
      country
    from
      dw_zaful_recommend.report_exp_tmp
    where
      glb_plf = 'pc'
      and glb_b = 'c'
      and mdlc = 'T_9'
  ) c on a.add_time = c.add_time
  and a.glb_dc = c.glb_dc
  and a.country = c.country
  left join (
    select
      click_num,
      add_time,
      glb_dc,
      country
    from
      dw_zaful_recommend.report_click_tmp
    where
      glb_plf = 'pc'
      and glb_b = 'c'
      and mdlc = 'T_9'
  ) d on a.add_time = d.add_time
  and a.glb_dc = d.glb_dc
  and a.country = d.country
  left join (
    select
      cart_num,
      add_time,
      glb_dc,
      country
    from
      dw_zaful_recommend.report_cart_tmp
    where
      glb_plf = 'pc'
      and fmd = 'mr_T_9'
  ) e on a.add_time = e.add_time
  and a.glb_dc = e.glb_dc
  and a.country = e.country
  left join (
    select
      order_num,
      add_time,
      glb_dc,
      country
    from
      dw_zaful_recommend.report_order_tmp
    where
      glb_plf = 'pc'
      and fmd = 'mr_T_9'
  ) f on a.add_time = f.add_time
  and a.glb_dc = f.glb_dc
  and a.country = f.country
  left join (
    select
      purchase_num,
      add_time,
      glb_dc,
      country
    from
      dw_zaful_recommend.report_purchase_tmp
    where
      glb_plf = 'pc'
      and fmd = 'mr_T_9'
  ) g on a.add_time = g.add_time
  and a.glb_dc = g.glb_dc
  and a.country = g.country
  left join (
    select
      pay_amount,
      add_time,
      glb_dc,
      country
    from
      dw_zaful_recommend.report_pay_amount_tmp
    where
      glb_plf = 'pc'
      and fmd = 'mr_T_9'
  ) h on a.add_time = h.add_time
  and a.glb_dc = h.glb_dc
  and a.country = h.country
  left join (
    select
      collect_num,
      add_time,
      glb_dc,
      country
    from
      dw_zaful_recommend.report_collect_tmp
    where
      glb_plf = 'pc'
      and fmd = 'mr_T_9'
  ) i on a.add_time = i.add_time
  and a.glb_dc = i.glb_dc
  and a.country = i.country
union all
select
  a.pv,
  b.uv,
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
  'PC',
  'T_5',
  '购物车页-FEATURED RECOMMENDATIONS',
  a.glb_dc,
  a.country
from
  (
    select
      pv,
      add_time,
      glb_dc,
      country
    from
      dw_zaful_recommend.report_pv_tmp
    where
      glb_plf = 'pc'
      and glb_b = 'd'
  ) a
  left join (
    select
      uv,
      add_time,
      glb_dc,
      country
    from
      dw_zaful_recommend.report_uv_tmp
    where
      glb_plf = 'pc'
      and glb_b = 'd'
  ) b on a.add_time = b.add_time
  and a.glb_dc = b.glb_dc
  and a.country = b.country
  left join (
    select
      exp_num,
      add_time,
      glb_dc,
      country
    from
      dw_zaful_recommend.report_exp_tmp
    where
      glb_plf = 'pc'
      and glb_b = 'd'
      and mdlc = 'T_5'
  ) c on a.add_time = c.add_time
  and a.glb_dc = c.glb_dc
  and a.country = c.country
  left join (
    select
      click_num,
      add_time,
      glb_dc,
      country
    from
      dw_zaful_recommend.report_click_tmp
    where
      glb_plf = 'pc'
      and glb_b = 'd'
      and mdlc = 'T_5'
  ) d on a.add_time = d.add_time
  and a.glb_dc = d.glb_dc
  and a.country = d.country
  left join (
    select
      cart_num,
      add_time,
      glb_dc,
      country
    from
      dw_zaful_recommend.report_cart_tmp
    where
      glb_plf = 'pc'
      and fmd = 'mr_T_5'
  ) e on a.add_time = e.add_time
  and a.glb_dc = e.glb_dc
  and a.country = e.country
  left join (
    select
      order_num,
      add_time,
      glb_dc,
      country
    from
      dw_zaful_recommend.report_order_tmp
    where
      glb_plf = 'pc'
      and fmd = 'mr_T_5'
  ) f on a.add_time = f.add_time
  and a.glb_dc = f.glb_dc
  and a.country = f.country
  left join (
    select
      purchase_num,
      add_time,
      glb_dc,
      country
    from
      dw_zaful_recommend.report_purchase_tmp
    where
      glb_plf = 'pc'
      and fmd = 'mr_T_5'
  ) g on a.add_time = g.add_time
  and a.glb_dc = g.glb_dc
  and a.country = g.country
  left join (
    select
      pay_amount,
      add_time,
      glb_dc,
      country
    from
      dw_zaful_recommend.report_pay_amount_tmp
    where
      glb_plf = 'pc'
      and fmd = 'mr_T_5'
  ) h on a.add_time = h.add_time
  and a.glb_dc = h.glb_dc
  and a.country = h.country
  left join (
    select
      collect_num,
      add_time,
      glb_dc,
      country
    from
      dw_zaful_recommend.report_collect_tmp
    where
      glb_plf = 'pc'
      and fmd = 'mr_T_5'
  ) i on a.add_time = i.add_time
  and a.glb_dc = i.glb_dc
  and a.country = i.country
union all
select
  a.pv,
  b.uv,
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
  'PC',
  'T_6',
  '购物车页-YOUR RECENT HISTORY',
  a.glb_dc,
  a.country
from
  (
    select
      pv,
      add_time,
      glb_dc,
      country
    from
      dw_zaful_recommend.report_pv_tmp
    where
      glb_plf = 'pc'
      and glb_b = 'd'
  ) a
  left join (
    select
      uv,
      add_time,
      glb_dc,
      country
    from
      dw_zaful_recommend.report_uv_tmp
    where
      glb_plf = 'pc'
      and glb_b = 'd'
  ) b on a.add_time = b.add_time
  and a.glb_dc = b.glb_dc
  and a.country = b.country
  left join (
    select
      exp_num,
      add_time,
      glb_dc,
      country
    from
      dw_zaful_recommend.report_exp_tmp
    where
      glb_plf = 'pc'
      and glb_b = 'd'
      and mdlc = 'T_6'
  ) c on a.add_time = c.add_time
  and a.glb_dc = c.glb_dc
  and a.country = c.country
  left join (
    select
      click_num,
      add_time,
      glb_dc,
      country
    from
      dw_zaful_recommend.report_click_tmp
    where
      glb_plf = 'pc'
      and glb_b = 'd'
      and mdlc = 'T_6'
  ) d on a.add_time = d.add_time
  and a.glb_dc = d.glb_dc
  and a.country = d.country
  left join (
    select
      cart_num,
      add_time,
      glb_dc,
      country
    from
      dw_zaful_recommend.report_cart_tmp
    where
      glb_plf = 'pc'
      and fmd = 'mr_T_6'
  ) e on a.add_time = e.add_time
  and a.glb_dc = e.glb_dc
  and a.country = e.country
  left join (
    select
      order_num,
      add_time,
      glb_dc,
      country
    from
      dw_zaful_recommend.report_order_tmp
    where
      glb_plf = 'pc'
      and fmd = 'mr_T_6'
  ) f on a.add_time = f.add_time
  and a.glb_dc = f.glb_dc
  and a.country = f.country
  left join (
    select
      purchase_num,
      add_time,
      glb_dc,
      country
    from
      dw_zaful_recommend.report_purchase_tmp
    where
      glb_plf = 'pc'
      and fmd = 'mr_T_6'
  ) g on a.add_time = g.add_time
  and a.glb_dc = g.glb_dc
  and a.country = g.country
  left join (
    select
      pay_amount,
      add_time,
      glb_dc,
      country
    from
      dw_zaful_recommend.report_pay_amount_tmp
    where
      glb_plf = 'pc'
      and fmd = 'mr_T_6'
  ) h on a.add_time = h.add_time
  and a.glb_dc = h.glb_dc
  and a.country = h.country
  left join (
    select
      collect_num,
      add_time,
      glb_dc,
      country
    from
      dw_zaful_recommend.report_collect_tmp
    where
      glb_plf = 'pc'
      and fmd = 'mr_T_6'
  ) i on a.add_time = i.add_time
  and a.glb_dc = i.glb_dc
  and a.country = i.country
union all
select
  a.pv,
  b.uv,
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
  'PC',
  'T_7',
  '购物车页-FAVORITE',
  a.glb_dc,
  a.country
from
  (
    select
      pv,
      add_time,
      glb_dc,
      country
    from
      dw_zaful_recommend.report_pv_tmp
    where
      glb_plf = 'pc'
      and glb_b = 'd'
  ) a
  left join (
    select
      uv,
      add_time,
      glb_dc,
      country
    from
      dw_zaful_recommend.report_uv_tmp
    where
      glb_plf = 'pc'
      and glb_b = 'd'
  ) b on a.add_time = b.add_time
  and a.glb_dc = b.glb_dc
  and a.country = b.country
  left join (
    select
      exp_num,
      add_time,
      glb_dc,
      country
    from
      dw_zaful_recommend.report_exp_tmp
    where
      glb_plf = 'pc'
      and glb_b = 'd'
      and mdlc = 'T_7'
  ) c on a.add_time = c.add_time
  and a.glb_dc = c.glb_dc
  and a.country = c.country
  left join (
    select
      click_num,
      add_time,
      glb_dc,
      country
    from
      dw_zaful_recommend.report_click_tmp
    where
      glb_plf = 'pc'
      and glb_b = 'd'
      and mdlc = 'T_7'
  ) d on a.add_time = d.add_time
  and a.glb_dc = d.glb_dc
  and a.country = d.country
  left join (
    select
      cart_num,
      add_time,
      glb_dc,
      country
    from
      dw_zaful_recommend.report_cart_tmp
    where
      glb_plf = 'pc'
      and fmd = 'mr_T_7'
  ) e on a.add_time = e.add_time
  and a.glb_dc = e.glb_dc
  and a.country = e.country
  left join (
    select
      order_num,
      add_time,
      glb_dc,
      country
    from
      dw_zaful_recommend.report_order_tmp
    where
      glb_plf = 'pc'
      and fmd = 'mr_T_7'
  ) f on a.add_time = f.add_time
  and a.glb_dc = f.glb_dc
  and a.country = f.country
  left join (
    select
      purchase_num,
      add_time,
      glb_dc,
      country
    from
      dw_zaful_recommend.report_purchase_tmp
    where
      glb_plf = 'pc'
      and fmd = 'mr_T_7'
  ) g on a.add_time = g.add_time
  and a.glb_dc = g.glb_dc
  and a.country = g.country
  left join (
    select
      pay_amount,
      add_time,
      glb_dc,
      country
    from
      dw_zaful_recommend.report_pay_amount_tmp
    where
      glb_plf = 'pc'
      and fmd = 'mr_T_7'
  ) h on a.add_time = h.add_time
  and a.glb_dc = h.glb_dc
  and a.country = h.country
  left join (
    select
      collect_num,
      add_time,
      glb_dc,
      country
    from
      dw_zaful_recommend.report_collect_tmp
    where
      glb_plf = 'pc'
      and fmd = 'mr_T_7'
  ) i on a.add_time = i.add_time
  and a.glb_dc = i.glb_dc
  and a.country = i.country
union all
select
  a.pv,
  b.uv,
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
  'PC',
  'T_8',
  '购物车页-YOU MIGHT ALSO LIKE',
  a.glb_dc,
  a.country
from
  (
    select
      pv,
      add_time,
      glb_dc,
      country
    from
      dw_zaful_recommend.report_pv_tmp
    where
      glb_plf = 'pc'
      and glb_b = 'd'
  ) a
  left join (
    select
      uv,
      add_time,
      glb_dc,
      country
    from
      dw_zaful_recommend.report_uv_tmp
    where
      glb_plf = 'pc'
      and glb_b = 'd'
  ) b on a.add_time = b.add_time
  and a.glb_dc = b.glb_dc
  and a.country = b.country
  left join (
    select
      exp_num,
      add_time,
      glb_dc,
      country
    from
      dw_zaful_recommend.report_exp_tmp
    where
      glb_plf = 'pc'
      and glb_b = 'd'
      and mdlc = 'T_8'
  ) c on a.add_time = c.add_time
  and a.glb_dc = c.glb_dc
  and a.country = c.country
  left join (
    select
      click_num,
      add_time,
      glb_dc,
      country
    from
      dw_zaful_recommend.report_click_tmp
    where
      glb_plf = 'pc'
      and glb_b = 'd'
      and mdlc = 'T_8'
  ) d on a.add_time = d.add_time
  and a.glb_dc = d.glb_dc
  and a.country = d.country
  left join (
    select
      cart_num,
      add_time,
      glb_dc,
      country
    from
      dw_zaful_recommend.report_cart_tmp
    where
      glb_plf = 'pc'
      and fmd = 'mr_T_8'
  ) e on a.add_time = e.add_time
  and a.glb_dc = e.glb_dc
  and a.country = e.country
  left join (
    select
      order_num,
      add_time,
      glb_dc,
      country
    from
      dw_zaful_recommend.report_order_tmp
    where
      glb_plf = 'pc'
      and fmd = 'mr_T_8'
  ) f on a.add_time = f.add_time
  and a.glb_dc = f.glb_dc
  and a.country = f.country
  left join (
    select
      purchase_num,
      add_time,
      glb_dc,
      country
    from
      dw_zaful_recommend.report_purchase_tmp
    where
      glb_plf = 'pc'
      and fmd = 'mr_T_8'
  ) g on a.add_time = g.add_time
  and a.glb_dc = g.glb_dc
  and a.country = g.country
  left join (
    select
      pay_amount,
      add_time,
      glb_dc,
      country
    from
      dw_zaful_recommend.report_pay_amount_tmp
    where
      glb_plf = 'pc'
      and fmd = 'mr_T_8'
  ) h on a.add_time = h.add_time
  and a.glb_dc = h.glb_dc
  and a.country = h.country
  left join (
    select
      collect_num,
      add_time,
      glb_dc,
      country
    from
      dw_zaful_recommend.report_collect_tmp
    where
      glb_plf = 'pc'
      and fmd = 'mr_T_8'
  ) i on a.add_time = i.add_time
  and a.glb_dc = i.glb_dc
  and a.country = i.country
union all
select
  a.pv,
  b.uv,
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
  'M',
  'T_1',
  '首页-Recommend for you',
  a.glb_dc,
  a.country
from
  (
    select
      pv,
      add_time,
      glb_dc,
      country
    from
      dw_zaful_recommend.report_pv_tmp
    where
      glb_plf = 'm'
      and glb_b = 'a'
  ) a
  left join (
    select
      uv,
      add_time,
      glb_dc,
      country
    from
      dw_zaful_recommend.report_uv_tmp
    where
      glb_plf = 'm'
      and glb_b = 'a'
  ) b on a.add_time = b.add_time
  and a.glb_dc = b.glb_dc
  and a.country = b.country
  left join (
    select
      exp_num,
      add_time,
      glb_dc,
      country
    from
      dw_zaful_recommend.report_exp_tmp
    where
      glb_plf = 'm'
      and glb_b = 'a'
      and mdlc = 'T_1'
  ) c on a.add_time = c.add_time
  and a.glb_dc = c.glb_dc
  and a.country = c.country
  left join (
    select
      click_num,
      add_time,
      glb_dc,
      country
    from
      dw_zaful_recommend.report_click_tmp
    where
      glb_plf = 'm'
      and glb_b = 'a'
      and mdlc = 'T_1'
  ) d on a.add_time = d.add_time
  and a.glb_dc = d.glb_dc
  and a.country = d.country
  left join (
    select
      cart_num,
      add_time,
      glb_dc,
      country
    from
      dw_zaful_recommend.report_cart_tmp
    where
      glb_plf = 'm'
      and fmd = 'mr_T_1'
  ) e on a.add_time = e.add_time
  and a.glb_dc = e.glb_dc
  and a.country = e.country
  left join (
    select
      order_num,
      add_time,
      glb_dc,
      country
    from
      dw_zaful_recommend.report_order_tmp
    where
      glb_plf = 'm'
      and fmd = 'mr_T_1'
  ) f on a.add_time = f.add_time
  and a.glb_dc = f.glb_dc
  and a.country = f.country
  left join (
    select
      purchase_num,
      add_time,
      glb_dc,
      country
    from
      dw_zaful_recommend.report_purchase_tmp
    where
      glb_plf = 'm'
      and fmd = 'mr_T_1'
  ) g on a.add_time = g.add_time
  and a.glb_dc = g.glb_dc
  and a.country = g.country
  left join (
    select
      pay_amount,
      add_time,
      glb_dc,
      country
    from
      dw_zaful_recommend.report_pay_amount_tmp
    where
      glb_plf = 'm'
      and fmd = 'mr_T_1'
  ) h on a.add_time = h.add_time
  and a.glb_dc = h.glb_dc
  and a.country = h.country
  left join (
    select
      collect_num,
      add_time,
      glb_dc,
      country
    from
      dw_zaful_recommend.report_collect_tmp
    where
      glb_plf = 'm'
      and fmd = 'mr_T_1'
  ) i on a.add_time = i.add_time
  and a.glb_dc = i.glb_dc
  and a.country = i.country
union all
select
  a.pv,
  b.uv,
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
  'M',
  'T_2',
  '商详页-We recommend',
  a.glb_dc,
  a.country
from
  (
    select
      pv,
      add_time,
      glb_dc,
      country
    from
      dw_zaful_recommend.report_pv_tmp
    where
      glb_plf = 'm'
      and glb_b = 'c'
  ) a
  left join (
    select
      uv,
      add_time,
      glb_dc,
      country
    from
      dw_zaful_recommend.report_uv_tmp
    where
      glb_plf = 'm'
      and glb_b = 'c'
  ) b on a.add_time = b.add_time
  and a.glb_dc = b.glb_dc
  and a.country = b.country
  left join (
    select
      exp_num,
      add_time,
      glb_dc,
      country
    from
      dw_zaful_recommend.report_exp_tmp
    where
      glb_plf = 'm'
      and glb_b = 'c'
      and mdlc = 'T_2'
  ) c on a.add_time = c.add_time
  and a.glb_dc = c.glb_dc
  and a.country = c.country
  left join (
    select
      click_num,
      add_time,
      glb_dc,
      country
    from
      dw_zaful_recommend.report_click_tmp
    where
      glb_plf = 'm'
      and glb_b = 'c'
      and mdlc = 'T_2'
  ) d on a.add_time = d.add_time
  and a.glb_dc = d.glb_dc
  and a.country = d.country
  left join (
    select
      cart_num,
      add_time,
      glb_dc,
      country
    from
      dw_zaful_recommend.report_cart_tmp
    where
      glb_plf = 'm'
      and fmd = 'mr_T_2'
  ) e on a.add_time = e.add_time
  and a.glb_dc = e.glb_dc
  and a.country = e.country
  left join (
    select
      order_num,
      add_time,
      glb_dc,
      country
    from
      dw_zaful_recommend.report_order_tmp
    where
      glb_plf = 'm'
      and fmd = 'mr_T_2'
  ) f on a.add_time = f.add_time
  and a.glb_dc = f.glb_dc
  and a.country = f.country
  left join (
    select
      purchase_num,
      add_time,
      glb_dc,
      country
    from
      dw_zaful_recommend.report_purchase_tmp
    where
      glb_plf = 'm'
      and fmd = 'mr_T_2'
  ) g on a.add_time = g.add_time
  and a.glb_dc = g.glb_dc
  and a.country = g.country
  left join (
    select
      pay_amount,
      add_time,
      glb_dc,
      country
    from
      dw_zaful_recommend.report_pay_amount_tmp
    where
      glb_plf = 'm'
      and fmd = 'mr_T_2'
  ) h on a.add_time = h.add_time
  and a.glb_dc = h.glb_dc
  and a.country = h.country
  left join (
    select
      collect_num,
      add_time,
      glb_dc,
      country
    from
      dw_zaful_recommend.report_collect_tmp
    where
      glb_plf = 'm'
      and fmd = 'mr_T_2'
  ) i on a.add_time = i.add_time
  and a.glb_dc = i.glb_dc
  and a.country = i.country;



INSERT overwrite table dw_zaful_recommend.zaful_recommend_position_exp
select
  *
FROM
  dw_zaful_recommend.zaful_recommend_position_report
where
  add_time = '${ADD_TIME}';