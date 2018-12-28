--@author wuchao
--@date 2018年12月17日 
--@desc  rg邮件推荐用户数据

SET mapred.job.name=wuc_email_emp_rg_users_onsale;
set mapred.job.queue.name=root.ai.offline;
SET mapred.max.split.size=128000000;
SET mapred.min.split.size=64000000;
SET mapred.min.split.size.per.node=64000000;
SET mapred.min.split.size.per.rack=64000000;
SET hive.exec.reducers.bytes.per.reducer = 64000000;
SET hive.merge.mapfiles=true;
SET hive.merge.mapredfiles= true;
SET hive.input.format=org.apache.hadoop.hive.ql.io.CombineHiveInputFormat;
SET hive.merge.size.per.task=256000000;
SET hive.exec.parallel = true;
set hive.support.concurrency=false;


--建表
CREATE TABLE IF NOT EXISTS dw_rg_recommend.rosegal_users_onsale(
	sku       string        COMMENT '商品id'
);


--rg邮件用户
INSERT OVERWRITE TABLE dw_rg_recommend.rosegal_users_onsale  
select 
  distinct goods_sn
 from  ods.ods_m_rosegal_eload_goods 
 where dt='${DATE}'
and is_on_sale =1 and is_delete=0 and goods_number>0
;