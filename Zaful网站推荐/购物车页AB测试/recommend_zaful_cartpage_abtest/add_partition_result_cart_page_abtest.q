--@author LIUQINGFAN
--@date 2018年5月17日 
--@desc  推荐结果表建分区

SET mapred.job.name='add_partition_result_cart_page_abtest';

USE dw_zaful_recommend;

Alter TABLE result_cart_page_abtest ADD IF NOT EXISTS PARTITION  (year=${YEAR},month=${MONTH},day=${DAY}) location  '/user/hive/warehouse/dw_zaful_recommend.db/result_cart_page_abtest/year=${YEAR}/month=${MONTH}/day=${DAY}'; 


Alter TABLE result_cart_page_abtest2 ADD IF NOT EXISTS PARTITION  (year=${YEAR},month=${MONTH},day=${DAY}) location  '/user/hive/warehouse/dw_zaful_recommend.db/result_cart_page_abtest2/year=${YEAR}/month=${MONTH}/day=${DAY}'; 