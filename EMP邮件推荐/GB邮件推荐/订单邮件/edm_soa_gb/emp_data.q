--@author LIUQINGFAN
--@date 2018年6月19日 
--@desc  gb邮件


SET mapred.job.name='gb_emp_data';
set mapred.job.queue.name=root.ai.offline; 
USE   dw_gearbest_recommend;


CREATE TABLE IF NOT EXISTS gb_pc_event_info_detail_exp_date(
	goods_sn     string,
	glb_oi     string,
	glb_u        string,
	glb_od       string,
	glb_w        int,
	glb_x         string,
	glb_tm        bigint,
	add_time      int,
	year string,
	month    string,
	day      string
	)
PARTITIONED BY(pdate string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\u0001'                                                                                   
LINES TERMINATED BY '\n'                                                                                          
STORED AS TEXTFILE;


INSERT OVERWRITE TABLE gb_pc_event_info_detail_exp_date partition (pdate=${DATE}) 
SELECT
	get_json_object(glb_skuinfo,'$.sku') goods_sn ,
	NVL(glb_oi,'') glb_oi, 
	NVL(glb_u,'') glb_u,
	NVL(glb_od,'') glb_od, 
	NVL(glb_w,0) glb_w, 
	NVL(glb_x,'') glb_x, 
	NVL(glb_tm,0) glb_tm,
	unix_timestamp(concat(year,month,day),'yyyyMMdd') add_time,
	year, 
	month,
	day
FROM
	stg.gb_pc_event_info
WHERE
	glb_t='ic' and get_json_object(glb_skuinfo,'$.sku') !='' AND concat(year,month,day) = ${DATE};
	

	
CREATE TABLE IF NOT EXISTS gb_order_order_goods_detail_exp(
	order_sn     string,
	goods_sn     string,
	user_id        bigint,
	update_time    int
	)
PARTITIONED BY(pdate string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\u0001'                                                                                   
LINES TERMINATED BY '\n'                                                                                          
STORED AS TEXTFILE;


INSERT OVERWRITE TABLE gb_order_order_goods_detail_exp partition (pdate=${DATE}) 
SELECT
	a.order_sn, 
	a.goods_sn, 
	a.user_id, 
	b.created_time  update_time
FROM
	stg.gb_order_order_goods a
JOIN(
	SELECT
		order_sn,
		created_time
	FROM
		stg.gb_order_order_info
	WHERE
		from_unixtime(created_time,'yyyyMMdd') = ${DATE}
	) b
ON
	a.order_sn = b.order_sn;
	
CREATE TABLE IF NOT EXISTS gb_goods_info_result_detail_exp
(	
	good_sn            string,
	goods_spu           string,
	id               int,
	level_1         int,
	level_2               int,
	level_3      int,
	level_4            int
	)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\u0001'                                                                                   
LINES TERMINATED BY '\n'                                                                                          
STORED AS TEXTFILE;


INSERT OVERWRITE TABLE gb_goods_info_result_detail_exp
SELECT
	NVL(good_sn,'') good_sn  ,
	goods_spu,
	NVL(id,0) id,
	NVL(level_1,0) level_1 ,
	NVL(level_2,0) level_2,
	NVL(level_3,0) level_3,
	NVL(level_4,0) level_4
FROM
	dw_gearbest_recommend.goods_info_result
GROUP BY
	good_sn,
	id,
	goods_spu,
	level_1,
	level_2,
	level_3,
	level_4;
	
	
	

