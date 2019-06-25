
--@author wuchao
--@date 2018年12月20日 
--@desc  Zaful App新需求以sku,cat_id导入mongodb


SET mapred.job.name=history_data_delete;
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

--算法数据表 保留7天数据
alter table dw_proj.app_zaful_sku_cat_id_no_country_category_wuc_wishlist_temp drop partition (add_time<='${ADD_TIME}');--20190410

alter table dw_proj.app_zaful_sku_cat_id_no_country_category_wuc_wishlist drop partition (add_time<='${ADD_TIME}');

alter table dw_proj.app_zaful_sku_cat_id_no_country_all_wuc_wishlist_temp drop partition (add_time<='${ADD_TIME}');

alter table dw_proj.app_zaful_sku_cat_id_no_country_all_wuc_wishlist drop partition (add_time<='${ADD_TIME}');

alter table dw_proj.app_zaful_sku_cat_id_country_category_wuc_wishlist_temp drop partition (add_time<='${ADD_TIME}');

alter table dw_proj.app_zaful_sku_cat_id_country_category_wuc_wishlist drop partition (add_time<='${ADD_TIME}');

alter table dw_proj.app_zaful_sku_cat_id_country_all_wuc_wishlist_temp drop partition (add_time<='${ADD_TIME}');

alter table dw_proj.app_zaful_sku_cat_id_country_all_wuc_wishlist drop partition (add_time<='${ADD_TIME}');

alter table dw_proj.zaful_app_category_planid_temp drop partition (add_time<='${ADD_TIME}');

alter table dw_proj.zaful_app_category_planid drop partition (add_time<='${ADD_TIME}');



alter table dw_proj.app_zaful_sku_statistics_country_fb_genders drop partition (add_time<='${ADD_TIME_W}');--2019-04-10

alter table dw_zaful_recommend.zaful_app_wuc_appsflyer_device_id_report_media_source_exp drop partition (add_time<='${ADD_TIME_W}');

alter table dw_proj.app_zaful_sku_statistics_country drop partition (add_time<='${ADD_TIME_W}');

alter table dw_proj.app_zaful_sku_statistics_country_recommend_homepage drop partition (add_time<='${ADD_TIME_W}');

--特征工程,保留一个月的用于计算
--基础信息
alter table temp_zaful_recommend.feature_items_base_info_ods_desc drop partition (year<=${YEAR},month<=${MONTH});

alter table temp_zaful_recommend.feature_items_base_info_ods drop partition (year<=${YEAR},month<=${MONTH});
--分国家统计信息
alter table  dw_zaful_recommend.feature_items_country_v2_2_ods drop partition (year<=${YEAR},month<=${MONTH});

alter table  dw_zaful_recommend.feature_items_country_v2_2_ods_info drop partition (year<=${YEAR},month<=${MONTH});

alter table dw_zaful_recommend.feature_items_country_v2_2_ods_info_three_days drop partition (year<=${YEAR},month<=${MONTH});

alter table dw_zaful_recommend.feature_items_country_v2_2_ods_info_seven_days drop partition (year<=${YEAR},month<=${MONTH});

alter table dw_zaful_recommend.feature_items_country_v2_2_ods_info_fifteen_days drop partition (year<=${YEAR},month<=${MONTH});

alter table dw_zaful_recommend.feature_items_country_v2_2_ods_info_thirty_days drop partition (year<=${YEAR},month<=${MONTH});

alter table dw_zaful_recommend.feature_items_v2_2_ods_info_new_label_country drop partition (year<=${YEAR},month<=${MONTH});

--不分国家统计信息
alter table dw_zaful_recommend.feature_items_v2_2_ods   drop partition (year<=${YEAR},month<=${MONTH});

alter table  dw_zaful_recommend.feature_items_v2_2_ods_info  drop partition (year<=${YEAR},month<=${MONTH});

alter table  dw_zaful_recommend.feature_items_v2_2_ods_info_three_days  drop partition (year<=${YEAR},month<=${MONTH});

alter table  dw_zaful_recommend.feature_items_v2_2_ods_info_seven_days  drop partition (year<=${YEAR},month<=${MONTH});

alter table  dw_zaful_recommend.feature_items_v2_2_ods_info_fifteen_days  drop partition (year<=${YEAR},month<=${MONTH});

alter table  dw_zaful_recommend.feature_items_v2_2_ods_info_thirty_days  drop partition (year<=${YEAR},month<=${MONTH});

alter table  dw_zaful_recommend.feature_items_v2_2_ods_info_new_label  drop partition (year<=${YEAR},month<=${MONTH});


--大禹平台
alter table  dw_zaful_recommend.zaful_app_abset_id_user_fb_cookieid_fb   drop partition (add_time<='${ADD_TIME}');
