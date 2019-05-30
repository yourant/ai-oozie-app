--@author XIONGJUN1
--@date 2019年5月29日 
--@desc  推荐结果表建分区

SET mapred.job.name='gb_detail2_result_fpg_add_partition';

USE dw_gearbest_recommend;

Alter TABLE gb_detail2_result_fpg ADD IF NOT EXISTS PARTITION  (year=${YEAR},month=${MONTH},day=${DAY}) location  '/user/hive/warehouse/dw_gearbest_recommend.db/gb_detail2_result_fpg/year=${YEAR}/month=${MONTH}/day=${DAY}'; 

