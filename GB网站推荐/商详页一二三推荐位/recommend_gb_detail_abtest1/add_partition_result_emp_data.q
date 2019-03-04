--@author ZHOUPEI
--@date 2018年5月17日 
--@desc  推荐结果表建分区

SET mapred.job.name='add_partition_result_emp_data';

USE dw_gearbest_recommend;

Alter TABLE result_gb_email_create ADD IF NOT EXISTS PARTITION  (year=${YEAR},month=${MONTH},day=${DAY}) location  '/user/hive/warehouse/dw_gearbest_recommend.db/result_gb_email_create/year=${YEAR}/month=${MONTH}/day=${DAY}'; 


Alter TABLE result_gb_email_payed ADD IF NOT EXISTS PARTITION  (year=${YEAR},month=${MONTH},day=${DAY}) location  '/user/hive/warehouse/dw_gearbest_recommend.db/result_gb_email_payed/year=${YEAR}/month=${MONTH}/day=${DAY}'; 

Alter TABLE result_gb_email_promptpay ADD IF NOT EXISTS PARTITION  (year=${YEAR},month=${MONTH},day=${DAY}) location  '/user/hive/warehouse/dw_gearbest_recommend.db/result_gb_email_promptpay/year=${YEAR}/month=${MONTH}/day=${DAY}'; 

Alter TABLE gb_result_detail_page_gtq ADD IF NOT EXISTS PARTITION  (year=${YEAR},month=${MONTH},day=${DAY}) location  '/user/hive/warehouse/dw_gearbest_recommend.db/gb_result_detail_page_gtq/year=${YEAR}/month=${MONTH}/day=${DAY}'; 

Alter TABLE gb_result_detail_1_page_gtq ADD IF NOT EXISTS PARTITION  (year=${YEAR},month=${MONTH},day=${DAY}) location  '/user/hive/warehouse/dw_gearbest_recommend.db/gb_result_detail_1_page_gtq/year=${YEAR}/month=${MONTH}/day=${DAY}'; 

Alter TABLE gb_result_detail_3_page_gtq ADD IF NOT EXISTS PARTITION  (year=${YEAR},month=${MONTH},day=${DAY}) location  '/user/hive/warehouse/dw_gearbest_recommend.db/gb_result_detail_3_page_gtq/year=${YEAR}/month=${MONTH}/day=${DAY}'; 
