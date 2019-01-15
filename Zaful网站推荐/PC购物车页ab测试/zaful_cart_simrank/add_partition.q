--@author zhanrui
--@date 2019年01月14日 
--@desc  推荐结果表建分区

SET mapred.job.name='add_partition_result_cart_simrank';
set mapreduce.job.queuename = root.ai.offline;

Alter TABLE dw_zaful_recommend.result_cart_simrank ADD IF NOT EXISTS PARTITION  (year=${YEAR},month=${MONTH},day=${DAY}) location  '/user/hive/warehouse/dw_zaful_recommend.db/result_cart_simrank/year=${YEAR}/month=${MONTH}/day=${DAY}';
