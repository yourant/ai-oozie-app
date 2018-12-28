
--@author wuchao
--@date 2018年12月11日 
--@desc  Zaful App购物车页


SET mapred.job.name=zaful_app_recommend_item_banner_all_wuc_report;
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
CREATE TABLE IF NOT EXISTS dw_zaful_recommend.zaful_app_recommend_item_banner_all_wuc_report(
pv                        INT            COMMENT "页面PV",
uv                        INT            COMMENT "页面UV",
position_click_num          INT            COMMENT "坑位点击",
position_uv               INT            COMMENT "坑位uv",
platform                  STRING         COMMENT "平台",
recommend_position        STRING         COMMENT "列表页编号"
)
COMMENT 'zaful APP发现好货报表新需求'
PARTITIONED BY (add_time STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\u0001' LINES TERMINATED BY '\n' STORED AS TEXTFILE
;




--页面PV 
CREATE TABLE IF NOT EXISTS dw_zaful_recommend.zaful_app_recommend_item_banner_all_wuc_pv_tmp(
pv                        INT            COMMENT "页面PV",
platform                  STRING         COMMENT "平台",
af_inner_mediasource      STRING         COMMENT "列表页",
add_time                  STRING         COMMENT "日期"
)
COMMENT "列表页报表页面PV中间表"
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\u0001' LINES TERMINATED BY '\n' STORED AS TEXTFILE;



--页面UV	
CREATE TABLE IF NOT EXISTS dw_zaful_recommend.zaful_app_recommend_item_banner_all_wuc_uv_tmp(
uv                        INT            COMMENT "页面UV",
platform                  STRING         COMMENT "平台",
af_inner_mediasource      STRING         COMMENT "列表页",
add_time                  STRING         COMMENT "日期"
)
COMMENT "列表页报表页面UV中间表"
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\u0001' LINES TERMINATED BY '\n' STORED AS TEXTFILE;

--坑位点击数	
CREATE TABLE IF NOT EXISTS dw_zaful_recommend.zaful_app_recommend_item_banner_all_wuc_position_click_num_tmp(
position_click_num                   INT            COMMENT "坑位曝光数",
platform                  STRING         COMMENT "平台",
af_inner_mediasource      STRING         COMMENT "列表页",
add_time                  STRING         COMMENT "日期"
)
COMMENT "列表页报表商品曝光数中间表"
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\u0001' LINES TERMINATED BY '\n' STORED AS TEXTFILE;

--坑位uv	
CREATE TABLE IF NOT EXISTS dw_zaful_recommend.zaful_app_recommend_item_banner_all_wuc_position_uv_tmp(
position_uv                   INT            COMMENT "坑位曝光数",
platform                  STRING         COMMENT "平台",
af_inner_mediasource      STRING         COMMENT "列表页",
add_time                  STRING         COMMENT "日期"
)
COMMENT "列表页报表商品曝光数中间表"
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\u0001' LINES TERMINATED BY '\n' STORED AS TEXTFILE;





--页面PV
INSERT OVERWRITE TABLE dw_zaful_recommend.zaful_app_recommend_item_banner_all_wuc_pv_tmp
SELECT
  count( m.appsflyer_device_id) AS pv,
  m.platform,
  m.af_inner_mediasource,
  '${ADD_TIME}' as add_time
FROM
  (
    SELECT
      appsflyer_device_id,
      platform,
      'Great Styles' as af_inner_mediasource,
      language,
      country_code
    FROM
      ods.ods_app_burial_log 
    WHERE
      concat_ws('-', year, month, day) = '${ADD_TIME}'
        and event_name='af_banner_impression'
        and get_json_object(event_value,'$.af_banner_name') ='Great Styles'
        and site='zaful'
  ) m
group by
  m.platform,
  m.af_inner_mediasource
  ;


--页面UV
INSERT OVERWRITE TABLE dw_zaful_recommend.zaful_app_recommend_item_banner_all_wuc_uv_tmp
SELECT
  count(distinct m.appsflyer_device_id) AS uv,
  m.platform,
  m.af_inner_mediasource,
  '${ADD_TIME}' as add_time
FROM
  (
    SELECT
      appsflyer_device_id,
      platform,
      'Great Styles' as af_inner_mediasource,
      language,
      country_code
    FROM
      ods.ods_app_burial_log 
    WHERE
      concat_ws('-', year, month, day) = '${ADD_TIME}'
        and event_name='af_banner_impression'
        and get_json_object(event_value,'$.af_banner_name') ='Great Styles'
        and site='zaful'
  ) m
group by
  m.platform,
  m.af_inner_mediasource
  ;

--坑位点击数
INSERT OVERWRITE TABLE  dw_zaful_recommend.zaful_app_recommend_item_banner_all_wuc_position_click_num_tmp
SELECT
count(m.appsflyer_device_id) as position_click_num,
m.platform,
m.af_inner_mediasource,
'${ADD_TIME}' as add_time
FROM
(
  SELECT
  appsflyer_device_id,
  platform,
  get_json_object(event_value, '$.af_banner_name') as af_inner_mediasource,
  language,
  country_code
  FROM
  ods.ods_app_burial_log 
  WHERE
  concat_ws('-', year, month, day) = '${ADD_TIME}'
  and event_name='af_banner_click'
  and get_json_object(event_value, '$.af_banner_name') ='Great Styles'
      and site='zaful'
) m 
group by
m.platform,
m.af_inner_mediasource
;



--坑位uv
INSERT OVERWRITE TABLE  dw_zaful_recommend.zaful_app_recommend_item_banner_all_wuc_position_uv_tmp
SELECT
count(distinct m.appsflyer_device_id) as position_uv,
m.platform,
m.af_inner_mediasource,
'${ADD_TIME}' as add_time
FROM
(
  SELECT
  appsflyer_device_id,
  platform,
  get_json_object(event_value, '$.af_banner_name') as af_inner_mediasource,
  language,
  country_code
  FROM
  ods.ods_app_burial_log 
  WHERE
  concat_ws('-', year, month, day) = '${ADD_TIME}'
  and event_name='af_banner_click'
  and get_json_object(event_value, '$.af_banner_name') ='Great Styles'
      and site='zaful'
) m 
group by
m.platform,
m.af_inner_mediasource
;

--所有结果汇总
INSERT OVERWRITE TABLE dw_zaful_recommend.zaful_app_recommend_item_banner_all_wuc_report PARTITION (add_time = '${ADD_TIME}')
select
  NVL(a.pv,0),
  NVL(b.uv,0),
  c.position_click_num,
  d.position_uv,
  'android' as platform,
  a.af_inner_mediasource

from   
(select pv, add_time,af_inner_mediasource from    dw_zaful_recommend.zaful_app_recommend_item_banner_all_wuc_pv_tmp where  platform='android') a 
left join
(select uv, add_time,af_inner_mediasource from    dw_zaful_recommend.zaful_app_recommend_item_banner_all_wuc_uv_tmp where  platform='android') b 
on a.add_time=b.add_time and a.af_inner_mediasource=b.af_inner_mediasource 
left join 
(select position_click_num, add_time,af_inner_mediasource  from   dw_zaful_recommend.zaful_app_recommend_item_banner_all_wuc_position_click_num_tmp where  platform='android') c 
on a.add_time=c.add_time and a.af_inner_mediasource=c.af_inner_mediasource 
left join 
(select position_uv, add_time,af_inner_mediasource  from   dw_zaful_recommend.zaful_app_recommend_item_banner_all_wuc_position_uv_tmp where  platform='android') d 
on a.add_time=d.add_time and a.af_inner_mediasource=d.af_inner_mediasource 


union all

select
  NVL(a.pv,0),
  NVL(b.uv,0),
  c.position_click_num,
  d.position_uv,
  'ios' as platform,
  a.af_inner_mediasource

from   
(select pv, add_time,af_inner_mediasource  from    dw_zaful_recommend.zaful_app_recommend_item_banner_all_wuc_pv_tmp where  platform='ios') a 
left join
(select uv, add_time,af_inner_mediasource  from    dw_zaful_recommend.zaful_app_recommend_item_banner_all_wuc_uv_tmp where  platform='ios') b 
on a.add_time=b.add_time and a.af_inner_mediasource=b.af_inner_mediasource 
left join 
(select position_click_num, add_time,af_inner_mediasource  from   dw_zaful_recommend.zaful_app_recommend_item_banner_all_wuc_position_click_num_tmp where  platform='ios') c 
on a.add_time=c.add_time and a.af_inner_mediasource=c.af_inner_mediasource 
left join 
(select position_uv, add_time,af_inner_mediasource  from   dw_zaful_recommend.zaful_app_recommend_item_banner_all_wuc_position_uv_tmp where  platform='ios') d 
on a.add_time=d.add_time and a.af_inner_mediasource=d.af_inner_mediasource 
;










