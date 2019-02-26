--@author LIUQINGFAN
--@date 2018年6月28日 
--@desc  推荐结果表建分区

SET mapred.job.name='add_partition_result_detail_uncookie_user';

USE dw_zaful_recommend;

Alter TABLE result_detail_cookie_user ADD IF NOT EXISTS PARTITION  (year=${YEAR},month=${MONTH},day=${DAY}) location  '/user/hive/warehouse/dw_zaful_recommend.db/result_detail_cookie_user/year=${YEAR}/month=${MONTH}/day=${DAY}'; 

Alter TABLE result_detail_uncookie_user ADD IF NOT EXISTS PARTITION  (year=${YEAR},month=${MONTH},day=${DAY}) location  '/user/hive/warehouse/dw_zaful_recommend.db/result_detail_uncookie_user/year=${YEAR}/month=${MONTH}/day=${DAY}'; 

Alter TABLE result_detail_nocookie_user_abtest ADD IF NOT EXISTS PARTITION  (year=${YEAR},month=${MONTH},day=${DAY}) location  '/user/hive/warehouse/dw_zaful_recommend.db/result_detail_nocookie_user_abtest/year=${YEAR}/month=${MONTH}/day=${DAY}'; 

Alter TABLE stg.ip_country_region_table ADD IF NOT EXISTS PARTITION  (year=${YEAR},month=${MONTH},day=${DAY}) location  '/user/hive/warehouse/stg.db/ip_country_region_table/year=${YEAR}/month=${MONTH}/day=${DAY}'; 




