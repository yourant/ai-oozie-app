--@author Liuqingfan
--@date 2018年7月20日 
--@desc  

SET mapred.job.name='zaful_real_rec_click';
set mapred.job.queue.name=root.ai.online; 
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
insert overwrite table apl_gb_recommend_10002_dev_db.zaful_real_rec_click
select
  glb_ksku,
  clicksku
from
  (
    select
      glb_ksku,
      get_json_object(glb_ubcta, '$.sku') clicksku,
      get_json_object(glb_ubcta, '$.mrlc') location
    from
      stg.zf_pc_event_info
    where
      concat(year, month, day) between ${ADD_TIME_W}
      and ${DATE}
      and glb_b = 'c'
      and glb_t = 'ic'
      and glb_ubcta <> ''
  ) a
where
  location = 'T_3';




