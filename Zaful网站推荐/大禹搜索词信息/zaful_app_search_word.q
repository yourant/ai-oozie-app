
--@author wuchao
--@date 2019年1月117日 
--@desc  王忠付大禹数据迁移


SET mapred.job.name=zaful_app_search_word;
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
CREATE TABLE IF NOT EXISTS dw_proj.zaful_app_search_word_fourteen_days_report(
     word_count          int                         COMMENT '词数',
     search_word         STRING                      COMMENT '词'
)
COMMENT '指标临时表'
 PARTITIONED BY (add_time STRING)
 ROW FORMAT DELIMITED FIELDS TERMINATED BY '\u0001' LINES TERMINATED BY '\n' STORED AS TEXTFILE
;





--所有结果汇总
INSERT overwrite TABLE dw_proj.zaful_app_search_word_fourteen_days_report PARTITION (add_time='${ADD_TIME}')
select * from (
select count(*) word_count,get_json_object(event_value,'$.af_content_type') as search_word
from ods.ods_app_burial_log 
where event_name = 'af_search' and site = 'gearbest' 
and concat_ws('-',year,month,day) BETWEEN '${ADD_TIME_W}'AND '${ADD_TIME}' 
group by concat_ws('-',year,month,day),get_json_object(event_value,'$.af_content_type')

) a where a.word_count > 10;
;














































































































































