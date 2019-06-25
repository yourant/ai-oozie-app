--@author wuchao
--@date 2019年03月05日 
--@desc  所有的cookieid,userid映射表

SET mapred.job.name=cookieid_userid_ods_zf_gb_rg_dL;
set mapred.job.queue.name=root.ai.offline;
SET mapred.max.split.size=128000000;
SET mapred.min.split.size=128000000;
SET mapred.min.split.size.per.node=128000000;
SET mapred.min.split.size.per.rack=128000000;
SET hive.exec.reducers.bytes.per.reducer = 128000000;
SET hive.merge.mapfiles=true;
SET hive.merge.mapredfiles= true;
SET hive.input.format=org.apache.hadoop.hive.ql.io.CombineHiveInputFormat;
SET hive.merge.size.per.task=128000000;
SET hive.exec.parallel = true;
set hive.support.concurrency=false;


--zaful app
-- INSERT OVERWRITE TABLE  dw_zaful_recommend.zaful_app_u_map
-- SELECT
-- m.appsflyer_device_id,
-- m.customer_user_id
-- FROM 
-- (
--   SELECT
--   appsflyer_device_id,
--   customer_user_id
--   FROM
--   dw_zaful_recommend.zaful_app_u_map
--   union all
--   SELECT
--   appsflyer_device_id,
--   customer_user_id
--   FROM
--   ods.ods_app_burial_log 
--   where 
--   concat(year, month, day) = '${DATE}'
--       AND customer_user_id <> '0'
--       AND customer_user_id <> ''
--       and site='zaful'
-- ) m
-- GROUP BY
-- m.appsflyer_device_id,
-- m.customer_user_id 
-- ;

--zaful pc/m
INSERT OVERWRITE TABLE  dw_zaful_recommend.ods_zaful_od_u_map
SELECT
m.cookie_id,
m.user_id
FROM 
(
  SELECT
  cookie_id,
  user_id
  FROM
  dw_zaful_recommend.ods_zaful_od_u_map
  union all
  SELECT
  cookie_id,
  user_id
  FROM
  ods.ods_pc_burial_log 
  where 
  concat(year, month, day) = '${DATE}'
      AND cookie_id <> '0'
      AND user_id <> ''
      and site='zaful'
  union all
  select
  cookie_id as cookie_id,
  user_id
  from 
  ods.ods_php_burial_log
  where
  concat(year, month, day) = '${DATE}'
  AND cookie_id <> '0'
      AND user_id <> ''
      and site='zaful'
) m
GROUP BY
m.cookie_id,
m.user_id 
;

--gb app
INSERT OVERWRITE TABLE  dw_gearbest_recommend.gb_app_u_map
SELECT
m.cookie_id,
m.user_id
FROM 
(
  SELECT
  cookie_id,
  user_id
  FROM
  dw_gearbest_recommend.gb_app_u_map
  union all
  SELECT
  appsflyer_device_id as cookie_id,
  customer_user_id as user_id
  FROM
  ods.ods_app_burial_log 
  where 
  concat(year, month, day) = '${DATE}'
      AND customer_user_id <> '0'
      AND customer_user_id <> ''
      and site='gearbest'
) m
GROUP BY
m.cookie_id,
m.user_id 
;

--gb pc/m
INSERT OVERWRITE TABLE  dw_gearbest_recommend.ods_gb_od_u_map
SELECT
m.cookie_id,
m.user_id
FROM 
(
  SELECT
  cookie_id,
  user_id
  FROM
  dw_gearbest_recommend.ods_gb_od_u_map
  union all
  SELECT
  cookie_id,
  user_id
  FROM
  ods.ods_pc_burial_log 
  where 
  concat(year, month, day) = '${DATE}'
      AND 
      cookie_id <> '0'
      AND user_id <> ''
      and site='gearbest'
) m
GROUP BY
m.cookie_id,
m.user_id 
;

--rg app
INSERT OVERWRITE TABLE  dw_rg_recommend.ods_rosegal_app_u_map
SELECT
m.appsflyer_device_id,
m.user_id
FROM 
(
  SELECT
  appsflyer_device_id,
  user_id
  FROM
  dw_rg_recommend.ods_rosegal_app_u_map
  union all
  SELECT
  appsflyer_device_id,
  customer_user_id as user_id
  FROM
  ods.ods_app_burial_log 
  where 
  concat(year, month, day) = '${DATE}'
      AND customer_user_id <> '0'
      AND customer_user_id <> ''
      and site='rosegal'
) m
GROUP BY
m.appsflyer_device_id,
m.user_id 
;


--rg pc/m
INSERT OVERWRITE TABLE  dw_proj.wuc_rg_od_u_map
SELECT m.cookie_id,m.user_id
FROM 
(
  SELECT cookie_id,user_id FROM dw_proj.wuc_rg_od_u_map
  union all
  SELECT cookie_id,user_id FROM ods.ods_pc_burial_log where concat(year, month, day) = '${DATE}' AND cookie_id <> '0' AND user_id <> '' and site='rosegal'
) m
GROUP BY m.cookie_id,m.user_id 
;

--dl app 暂时没有
INSERT OVERWRITE TABLE  dw_proj.ods_dresslily_app_u_map
SELECT
m.appsflyer_device_id,
m.user_id,
'${DATE}'
FROM 
(
  SELECT
  appsflyer_device_id,
  user_id
  FROM
  dw_proj.ods_dresslily_app_u_map
  union all
  SELECT
  appsflyer_device_id,
  customer_user_id as user_id
  FROM
  ods.ods_app_burial_log 
  where 
  concat(year, month, day) = '${DATE}'
      AND customer_user_id <> '0'
      AND customer_user_id <> ''
      and site='dresslily'
) m
GROUP BY
m.appsflyer_device_id,
m.user_id 
;

--dl pc/m

INSERT OVERWRITE TABLE  dw_proj.ods_dresslily_od_u_map
SELECT m.cookie_id,m.user_id
FROM 
(
  SELECT cookie_id,user_id FROM dw_proj.ods_dresslily_od_u_map
  union all
  SELECT cookie_id,user_id FROM ods.ods_pc_burial_log where  concat(year, month, day) = '${DATE}'  AND cookie_id <> '0' AND user_id <> '' and site='dresslily'
  union all
  select cookie_id as cookie_id,user_id from  ods.ods_php_burial_log where concat(year, month, day) = '${DATE}' AND cookie_id <> '0' AND user_id <> '' and site='dresslily'
) m
GROUP BY m.cookie_id,m.user_id 
;

--生成done_flag
  create table if not exists dw_proj.cookieid_userid_ods_zf_gb_rg_dL(
 dd                        STRING    comment ''
 )
 PARTITIONED BY (add_time STRING)
 ROW FORMAT DELIMITED FIELDS TERMINATED BY '\u0001' LINES TERMINATED BY '\n' STORED AS TEXTFILE
 ;
 

 --zhanrui  stg层映射表
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
      concat( year, month, day) = '${DATE}'
      AND glb_u rlike '^[0-9]+$'
  ) m
GROUP BY
  m.glb_od,
  m.glb_u
;

INSERT OVERWRITE TABLE dw_gearbest_report.gb_od_u_map
SELECT
  m.glb_od,
  m.glb_u
from
  (
    SELECT
      glb_od,
      glb_u
    FROM
      dw_gearbest_report.gb_od_u_map
    union all
    SELECT
      glb_od,
      glb_u
    FROM
      stg.gb_pc_event_info
    WHERE
      concat(year, month, day) = '${DATE}'
      AND glb_u rlike '^[0-9]+$'
  ) m
GROUP BY
  m.glb_od,
  m.glb_u
;

insert overwrite table dw_proj.cookieid_userid_ods_zf_gb_rg_dL  PARTITION (add_time='${DATE}')
select  '${DATE}'
;
