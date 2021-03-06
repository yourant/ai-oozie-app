--@author ZhanRui
--@date 2018年12月14日 
--@desc  zaful邮件推荐基础数据-使用ODS表

SET mapred.job.name=ods_zaful_recommend_base_data;
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






--埋点行为
INSERT overwrite TABLE dw_zaful_recommend.email_zaful_pc_event_info PARTITION (date = '${DATE}') SELECT
	get_json_object (skuinfo, '$.sku') AS goods_sn,
	session_id AS glb_oi,
	CASE
WHEN user_id = '' THEN
	0
ELSE
	user_id
END AS glb_u,
 cookie_id AS glb_od,
 page_stay_time AS glb_w,
 sub_event_info AS glb_x,
 time_stamp AS glb_tm,
 YEAR,
 MONTH,
 DAY,
 unix_timestamp(
	concat(YEAR, MONTH, DAY),
	'yyyyMMdd'
) AS add_time
FROM
	ods.ods_pc_burial_log
WHERE
	concat(YEAR, MONTH, DAY) = '${DATE}'
AND behaviour_type = 'ic'
AND skuinfo <> ''
AND site = 'zaful';


--订单信息
INSERT overwrite TABLE dw_zaful_recommend.email_zaful_goods_event_info PARTITION (date = '${DATE}') SELECT
	a.order_id,
	a.order_status,
	b.goods_sn,
	a.user_id,
	a.add_time
FROM
	(
		SELECT
			order_id,
			user_id,
			add_time,
			order_status
		FROM
			ods.ods_m_zaful_eload_order_info
		WHERE
			dt = '${DATE}'
		AND from_unixtime(add_time + 8 * 3600, 'yyyyMMdd') = '${DATE}'
	) a
JOIN (
	SELECT
		order_id,
		goods_sn
	FROM
		ods.ods_m_zaful_eload_order_goods
	WHERE
		dt = '${DATE}'
	AND from_unixtime(addtime + 8 * 3600, 'yyyyMMdd') = '${DATE}'
) b ON a.order_id = b.order_id;


CREATE TABLE IF NOT EXISTS tmp.apl_nodetree_zf_fact( 
	cat_id      string        COMMENT 'sku分类cat_id',
	cat_name    string        COMMENT '分类名称',
	node1       string        COMMENT '一级分类',
	node2       string        COMMENT '二级分类',
	node3       string        COMMENT '三级分类',
	node4       string        COMMENT '四级分类',
	dt       	string        COMMENT '更新日期'
)
COMMENT '商品等级分类表'
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\u0001'                                                                                   
LINES TERMINATED BY '\n'                                                                                          
STORED AS TEXTFILE;


--商品等级分类表(基础数据供使用)
INSERT OVERWRITE TABLE  tmp.apl_nodetree_zf_fact
SELECT 
	cat_id, 
	cat_name,
	split(d.NODE,',')[0] node1, --一级分类
	split(d.NODE,',')[1] node2, --二级分类
	split(d.NODE,',')[2] node3, --三级分类
	split(d.NODE,',')[3] node4,  --四级分类
	dt
FROM 
    ods.ods_m_zaful_eload_category d
WHERE d.dt='${DATE}'
;


--过滤上下架无库存
INSERT overwrite TABLE dw_zaful_recommend.email_emp_zaful_onsale
SELECT
    goods_sn,
    cat_id,
    catelist,
    node1,
    node2,
    node3,
    node4
FROM (
    SELECT 
        a.goods_sn,
        a.cat_id,
        CONCAT_WS('\\;',b.node1,b.node2,b.node3) as catelist,
        b.node1,
        b.node2,
        b.node3,
        b.node4
    FROM (
        SELECT 
            goods_sn,
            cat_id 
        FROM 
            ods.ods_m_zaful_eload_goods 
        WHERE
    	    dt = '${DATE}' 
    	AND is_on_sale = 1
        AND is_delete = 0
        AND goods_number > 0
        ) a 
    join 
        tmp.apl_nodetree_zf_fact b 
    on
        a.cat_id = b.cat_id) temp;