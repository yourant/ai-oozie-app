--@author ZHOUPEI
--@date 2018年5月17日 
--@desc  推荐结果表建分区

SET mapred.job.name='add_partition_result_goodsdetail_lr';

USE dw_zaful_recommend;

Alter TABLE result_goodsdetail_lr ADD IF NOT EXISTS PARTITION  (year=${YEAR},month=${MONTH},day=${DAY}) location  '/user/hive/warehouse/dw_zaful_recommend.db/result_goodsdetail_lr/year=${YEAR}/month=${MONTH}/day=${DAY}'; 


Alter TABLE result_cartdetail_lr ADD IF NOT EXISTS PARTITION  (year=${YEAR},month=${MONTH},day=${DAY}) location  '/user/hive/warehouse/dw_zaful_recommend.db/result_cartdetail_lr/year=${YEAR}/month=${MONTH}/day=${DAY}'; 

Alter TABLE result_detail_page_abtest_v3 ADD IF NOT EXISTS PARTITION  (year=${YEAR},month=${MONTH},day=${DAY}) location  '/user/hive/warehouse/dw_zaful_recommend.db/result_detail_page_abtest_v3/year=${YEAR}/month=${MONTH}/day=${DAY}'; 


Alter TABLE result_detail_embedding_gtq ADD IF NOT EXISTS PARTITION  (year=${YEAR},month=${MONTH},day=${DAY}) location  '/user/hive/warehouse/dw_zaful_recommend.db/result_detail_embedding_gtq/year=${YEAR}/month=${MONTH}/day=${DAY}';

Alter TABLE result_detail_page_abtest_v4 ADD IF NOT EXISTS PARTITION  (year=${YEAR},month=${MONTH},day=${DAY}) location  '/user/hive/warehouse/dw_zaful_recommend.db/result_detail_page_abtest_v4/year=${YEAR}/month=${MONTH}/day=${DAY}';

Alter TABLE result_detail_page_abtest_v5 ADD IF NOT EXISTS PARTITION  (year=${YEAR},month=${MONTH},day=${DAY}) location  '/user/hive/warehouse/dw_zaful_recommend.db/result_detail_page_abtest_v5/year=${YEAR}/month=${MONTH}/day=${DAY}';

Alter TABLE result_detail_page_abtest_w2v ADD IF NOT EXISTS PARTITION  (year=${YEAR},month=${MONTH},day=${DAY}) location  '/user/hive/warehouse/dw_zaful_recommend.db/result_detail_page_abtest_w2v/year=${YEAR}/month=${MONTH}/day=${DAY}';