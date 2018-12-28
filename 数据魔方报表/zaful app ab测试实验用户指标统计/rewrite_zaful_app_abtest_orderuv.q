--@author wuchao
--@date 2018年10月24日 
--@desc  zaful App端推荐位报表指标统计实验用户下单uv，下单金额，付款uv，付款金额

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


--输出结果表
CREATE TABLE IF NOT EXISTS dw_zaful_recommend.rewrite_zaful_app_abtest_orderuv_report(
order_uv                 INT            COMMENT "订单uv",
order_amount             INT            COMMENT "订单金额",
pay_uv                   INT            COMMENT "付款uv",
pay_amount               INT            COMMENT "付款金额",
platform                 STRING         COMMENT "android或者ios",
planid                   STRING         COMMENT "",
versionid                STRING         comment ""
)
COMMENT 'zaful APP推荐位数据报表新需求实验用户下单指标统计'
PARTITIONED BY (add_time STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\u0001' LINES TERMINATED BY '\n' STORED AS TEXTFILE
;

--下单uv表
CREATE TABLE IF NOT EXISTS dw_zaful_recommend.rewrite_zaful_app_abtest_orderuv_order_uv_tmp(
order_uv                 INT            COMMENT "订单uv",
platform                 STRING         COMMENT "android或者ios",
planid                   STRING         COMMENT "",
versionid                STRING         comment "",
add_time                  STRING         COMMENT "日期"
)
COMMENT 'zaful APP推荐位数据报表新需求实验用户order_uv'
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\u0001' LINES TERMINATED BY '\n' STORED AS TEXTFILE
;

--下单金额表
CREATE TABLE IF NOT EXISTS dw_zaful_recommend.rewrite_zaful_app_abtest_orderuv_order_amount_tmp(
order_amount                 INT            COMMENT "订单金额",
platform                 STRING         COMMENT "android或者ios",
planid                   STRING         COMMENT "",
versionid                STRING         comment "",
add_time                  STRING         COMMENT "日期"
)
COMMENT 'zaful APP推荐位数据报表新需求实验用户order_amount'
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\u0001' LINES TERMINATED BY '\n' STORED AS TEXTFILE
;


--付款uv表
CREATE TABLE IF NOT EXISTS dw_zaful_recommend.rewrite_zaful_app_abtest_orderuv_pay_uv_tmp(
pay_uv                   INT            COMMENT "付款uv",
platform                 STRING         COMMENT "android或者ios",
planid                   STRING         COMMENT "",
versionid                STRING         comment "",
add_time                  STRING         COMMENT "日期"
)
COMMENT 'zaful APP推荐位数据报表新需求实验用户pay_uv'
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\u0001' LINES TERMINATED BY '\n' STORED AS TEXTFILE
;

--付款金额表
CREATE TABLE IF NOT EXISTS dw_zaful_recommend.rewrite_zaful_app_abtest_orderuv_pay_amount_tmp(
pay_amount                 INT            COMMENT "付款金额",
platform                 STRING         COMMENT "android或者ios",
planid                   STRING         COMMENT "",
versionid                STRING         comment "",
add_time                  STRING         COMMENT "日期"
)
COMMENT 'zaful APP推荐位数据报表新需求实验用户pay_amount'
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\u0001' LINES TERMINATED BY '\n' STORED AS TEXTFILE
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

--中间表rewrite-uv_report_sku_user_tmp：取sku,user_id
CREATE TABLE IF NOT EXISTS dw_zaful_recommend.rewrite_zaful_app_abtest_orderuv_userid_deviceid_tmp(
user_id                   STRING         COMMENT "用户ID",
appsflyer_device_id       STRING         COMMENT "用户uv",
platform                  STRING         COMMENT "平台",
planid                   STRING         COMMENT "",
versionid                STRING         comment ""
)
COMMENT "推荐位报表userid-deviceid中间表"
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\u0001' LINES TERMINATED BY '\n' STORED AS TEXTFILE;


--中间表report_sku_user_tmp：取sku,user_id
INSERT OVERWRITE TABLE  dw_zaful_recommend.rewrite_zaful_app_abtest_orderuv_userid_deviceid_tmp
SELECT
  n.customer_user_id,
  m.appsflyer_device_id,
  m.platform,
  m.planid,
  m.versionid
FROM
  (
    SELECT
      appsflyer_device_id,
      platform,
	  get_json_object(event_value, '$.af_plan_id') as planid,
	  get_json_object(event_value, '$.af_version_id') as versionid
    FROM
      ods.ods_app_burial_log 
    WHERE
      concat_ws('-', year, month, day) = '${ADD_TIME}'
      and site='zaful'
  ) m
  INNER JOIN dw_zaful_recommend.zaful_app_u_map n ON m.appsflyer_device_id = n.appsflyer_device_id
GROUP BY
  n.customer_user_id,
  m.appsflyer_device_id,
  m.platform,
  m.planid,
  m.versionid
;


--中间表
CREATE TABLE IF NOT EXISTS dw_zaful_recommend.rewrite_zaful_app_abtest_orderuv_userid_orderid_tmp(
user_id                   STRING         COMMENT "用户ID",
order_status              STRING         COMMENT "订单状态",
pay_amount                decimal(10,2)  COMMENT "购买金额",
goods_number              STRING         COMMENT "商品数量",
order_id                  STRING         COMMENT ""
)
COMMENT "推荐位报表userid-orderid中间表"
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\u0001' LINES TERMINATED BY '\n' STORED AS TEXTFILE;


--中间表report_sku_user_tmp：取sku,user_id
INSERT OVERWRITE TABLE dw_zaful_recommend.rewrite_zaful_app_abtest_orderuv_userid_orderid_tmp
SELECT
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



--下单UV
INSERT OVERWRITE TABLE dw_zaful_recommend.rewrite_zaful_app_abtest_orderuv_order_uv_tmp
SELECT
  count(distinct x1.appsflyer_device_id) AS order_uv,
  x1.platform,
  x1.planid,
  x1.versionid,
  '${ADD_TIME}' as add_time
from
  dw_zaful_recommend.rewrite_zaful_app_abtest_orderuv_userid_deviceid_tmp x1
  INNER JOIN dw_zaful_recommend.rewrite_zaful_app_abtest_orderuv_userid_orderid_tmp x2 ON x1.user_id = x2.user_id
group by
  x1.platform,
  x1.planid,
  x1.versionid
;



--下单金额
INSERT OVERWRITE TABLE dw_zaful_recommend.rewrite_zaful_app_abtest_orderuv_order_amount_tmp
SELECT
  sum(x2.pay_amount) as order_amount,
  x1.platform,
  x1.planid,
  x1.versionid,
  '${ADD_TIME}' as add_time
from
  dw_zaful_recommend.rewrite_zaful_app_abtest_orderuv_userid_deviceid_tmp x1
  INNER JOIN dw_zaful_recommend.rewrite_zaful_app_abtest_orderuv_userid_orderid_tmp x2 ON x1.user_id = x2.user_id
group by
  x1.platform,
  x1.planid,
  x1.versionid
  ;



--付款UV
INSERT OVERWRITE TABLE dw_zaful_recommend.rewrite_zaful_app_abtest_orderuv_pay_uv_tmp
SELECT
  count(distinct x1.appsflyer_device_id) AS pay_uv,
  x1.platform,
  x1.planid,
  x1.versionid,
  '${ADD_TIME}' as add_time
from
  dw_zaful_recommend.rewrite_zaful_app_abtest_orderuv_userid_deviceid_tmp x1
  INNER JOIN dw_zaful_recommend.rewrite_zaful_app_abtest_orderuv_userid_orderid_tmp x2 ON x1.user_id = x2.user_id
where
  x2.order_status not in ('0', '11')
group by
  x1.platform,
  x1.planid,
  x1.versionid
;

--付款金额
INSERT OVERWRITE TABLE dw_zaful_recommend.rewrite_zaful_app_abtest_orderuv_pay_amount_tmp
SELECT
  sum(x2.pay_amount) as pay_amount,
  x1.platform,
  x1.planid,
  x1.versionid,
  '${ADD_TIME}' as add_time
from
  dw_zaful_recommend.rewrite_zaful_app_abtest_orderuv_userid_deviceid_tmp x1
  INNER JOIN dw_zaful_recommend.rewrite_zaful_app_abtest_orderuv_userid_orderid_tmp x2 ON x1.user_id = x2.user_id
where
  x2.order_status not in ('0', '11')
group by
  x1.platform,
  x1.planid,
  x1.versionid
  ;

--所有结果汇总
INSERT into TABLE dw_zaful_recommend.rewrite_zaful_app_abtest_orderuv_report PARTITION (add_time = '${ADD_TIME}')
select
  a.order_uv,
  b.order_amount,
  c.pay_uv,
  d.pay_amount,
  'android',
  a.planid,
  a.versionid
from 
    (select order_uv,platform,planid,versionid,add_time from dw_zaful_recommend.rewrite_zaful_app_abtest_orderuv_order_uv_tmp where  platform='android' ) a
    join
    (select order_amount,platform,planid,versionid,add_time from dw_zaful_recommend.rewrite_zaful_app_abtest_orderuv_order_amount_tmp where  platform='android' ) b
    on a.add_time=b.add_time and a.planid=b.planid and a.versionid=b.versionid
    join
    (select pay_uv,platform,planid,versionid,add_time from dw_zaful_recommend.rewrite_zaful_app_abtest_orderuv_pay_uv_tmp where  platform='android' ) c
    on a.add_time=c.add_time and a.planid=c.planid and a.versionid=c.versionid
    join
    (select pay_amount,platform,planid,versionid,add_time from dw_zaful_recommend.rewrite_zaful_app_abtest_orderuv_pay_amount_tmp where  platform='android' ) d
    on a.add_time=d.add_time and a.planid=d.planid and a.versionid=d.versionid
union all
select
  a.order_uv,
  b.order_amount,
  c.pay_uv,
  d.pay_amount,
  'ios',
  a.planid,
  a.versionid
from 
    (select order_uv,platform,planid,versionid,add_time from dw_zaful_recommend.rewrite_zaful_app_abtest_orderuv_order_uv_tmp where  platform='ios' ) a
    join
    (select order_amount,platform,planid,versionid,add_time from dw_zaful_recommend.rewrite_zaful_app_abtest_orderuv_order_amount_tmp where  platform='ios' ) b
    on a.add_time=b.add_time and a.planid=b.planid and a.versionid=b.versionid
    join
    (select pay_uv,platform,planid,versionid,add_time from dw_zaful_recommend.rewrite_zaful_app_abtest_orderuv_pay_uv_tmp where  platform='ios' ) c
    on a.add_time=c.add_time and a.planid=c.planid and a.versionid=c.versionid
    join
    (select pay_amount,platform,planid,versionid,add_time from dw_zaful_recommend.rewrite_zaful_app_abtest_orderuv_pay_amount_tmp where  platform='ios' ) d
    on a.add_time=d.add_time and a.planid=d.planid and a.versionid=d.versionid


