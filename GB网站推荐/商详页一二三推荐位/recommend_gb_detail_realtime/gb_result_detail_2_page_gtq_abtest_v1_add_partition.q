--@author XIONGJUN1
--@date 2019年5月29日 
--@desc  推荐结果表建分区

SET mapred.job.name='gb_result_detail_2_page_gtq_abtest_v1_add_partition';

USE dw_gearbest_recommend;

-- CREATE TABLE IF NOT EXISTS dw_gearbest_recommend.gb_result_detail_2_page_gtq_abtest_v1(
-- 	goods_sn1          string     COMMENT '主商品SKU',
-- 	goods_sn2          string     COMMENT '推荐商品SKU',
-- 	score      int     COMMENT '排序'
-- 	)
-- COMMENT 'gb商详页第二推荐位算法'
-- PARTITIONED BY ( 
-- 	`year` string, 
-- 	`month` string, 
-- 	`day` string)
-- ROW FORMAT SERDE 
-- 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe' 
-- WITH SERDEPROPERTIES ( 
-- 	'field.delim'='\u0001', 
-- 	'line.delim'='\n', 
-- 	'serialization.format'='\u0001');

-- CREATE TABLE IF NOT EXISTS dw_gearbest_recommend.gb_detail2_result_fpg(
-- 	goods_sn1          string     COMMENT '主商品SKU',
-- 	goods_sn2          string     COMMENT '推荐商品SKU',
-- 	score      int     COMMENT '排序'
-- 	)
-- COMMENT 'gb商详页第二推荐位算法2'
-- PARTITIONED BY ( 
-- 	`year` string, 
-- 	`month` string, 
-- 	`day` string)
-- ROW FORMAT SERDE 
-- 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe' 
-- WITH SERDEPROPERTIES ( 
-- 	'field.delim'='\u0001', 
-- 	'line.delim'='\n', 
-- 	'serialization.format'='\u0001');


Alter TABLE gb_result_detail_2_page_gtq_abtest_v1 ADD IF NOT EXISTS PARTITION  (year=${YEAR},month=${MONTH},day=${DAY}) location  '/user/hive/warehouse/dw_gearbest_recommend.db/gb_result_detail_2_page_gtq_abtest_v1/year=${YEAR}/month=${MONTH}/day=${DAY}'; 
