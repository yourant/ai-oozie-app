--@author wuchao
--@date 2018年12月17日 
--@desc  rg邮件推荐用户数据

SET mapred.job.name=wuc_email_emp_rg_users;
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

--用户id，国家对应表
-- CREATE TABLE IF NOT EXISTS dw_rg_recommend.rosegal_uc_map_wuc(
-- 	user_id       String        COMMENT '用户id',
-- 	country_code  String        COMMENT '国家简码',
-- 	country_name  String        COMMENT '国家名称'
-- );

-- INSERT OVERWRITE TABLE dw_rg_recommend.rosegal_uc_map_wuc
-- SELECT
-- m.user_id,
-- m.country_code,
-- m.country_name
-- FROM 
-- (
--   SELECT
--   user_id,
--   country_code,
--   country_name
--   FROM
--   dw_rg_recommend.rosegal_uc_map_wuc
--   union all
--   SELECT
--   user_id,
--   country_code,
--   country_name
--   FROM
--   ods.ods_pc_burial_log 
--   where 
--   concat_ws('-', year, month, day) BETWEEN '${ADD_TIME_W}' and '${ADD_TIME}'
--       AND user_id is not null
--       AND user_id <> ''
-- 	  and country_code is not null
--       and site='rosegal'
-- ) m
-- GROUP BY
-- m.user_id,
-- m.country_code,
-- m.country_name
-- ;  




-- --建表
-- CREATE TABLE IF NOT EXISTS dw_rg_recommend.email_emp_rosegal_users(
-- 	user_id       BIGINT        COMMENT '用户id',
-- 	country_code  String        COMMENT '国家简码'
-- );


-- --rg邮件用户
-- INSERT OVERWRITE TABLE dw_rg_recommend.email_emp_rosegal_users  
-- select 
-- x.user_id,
-- t.country_code
-- from (
-- 		SELECT
-- 		distinct a.user_id	
-- 		FROM
-- 			(
-- 				SELECT
-- 					user_id
-- 				FROM
-- 					ods.ods_m_rosegal_eload_order_info
-- 				WHERE
-- 					dt = '${DATE}'
-- 				AND FROM_UNIXTIME(pay_time + 8 * 3600, 'yyyyMMdd') BETWEEN '${DATE_W}'
-- 				AND '${DATE}'
-- 				AND order_status IN (1, 2, 3, 4, 6, 8, 15, 16, 20)
-- 			) a
-- 		JOIN (
-- 			SELECT
-- 				user_id
-- 			FROM
-- 				ods.ods_m_rosegal_eload_users
-- 			WHERE
-- 				dt = '${DATE}'
-- 			AND is_dingyue_success = 1
-- 		) b ON a.user_id = b.user_id
--      ) x
-- left join dw_rg_recommend.rosegal_uc_map_wuc t on x.user_id=t.user_id
-- ;



--建表
-- CREATE TABLE IF NOT EXISTS dw_rg_recommend.rosegal_users_onsale(
-- 	sku       string        COMMENT '商品id'
-- );


--rg过滤商品
INSERT OVERWRITE TABLE dw_rg_recommend.rosegal_users_onsale  
select 
   goods_sn
 from  ods.ods_m_rosegal_eload_goods 
 where dt='${DATE}'
and is_on_sale =1 and is_delete=0 and goods_number>0
group by goods_sn
;



-- select count(distinct(a.user_id)) as total  
-- from ods.ods_m_rosegal_eload_order_info as a 
-- join ods.ods_m_rosegal_eload_users as b on b.user_id = a.user_id
-- where 
--  FROM_UNIXTIME(a.pay_time + 8 * 3600, 'yyyyMMdd') BETWEEN '${DATE_W}'
-- 				AND '${DATE}'
-- and a.dt='20181219'
-- and b.dt='20181219'
-- and a.order_status in (1, 2, 3, 4, 6, 8, 15, 16, 20) 
-- and b.is_dingyue_success = 1