
--@author ZhanRui
--@date 2018年10月23日 
--@desc  gb 所有在售SKU统计

SET mapred.job.name=gb_sales_list_detail;
set mapred.job.queue.name=root.ai.offline;
SET mapred.max.split.size=128000000;
SET mapred.min.split.size=32000000;
SET mapred.min.split.size.per.node=32000000;
SET mapred.min.split.size.per.rack=32000000;
SET hive.exec.reducers.bytes.per.reducer = 128000000;
SET hive.merge.mapfiles=true;
SET hive.merge.mapredfiles= true;
SET hive.input.format=org.apache.hadoop.hive.ql.io.CombineHiveInputFormat;
SET hive.merge.size.per.task=256000000;
SET hive.exec.parallel = true; 

INSERT overwrite TABLE dw_gearbest_recommend.gb_sales_list_detail SELECT DISTINCT
	good_sn
FROM
	dw_gearbest_recommend.goods_info_result_uniq
;