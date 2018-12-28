--@author ZhanRui
--@date 2018年9月1日 
--@desc  Gb标签系统商品池数据

SET mapred.job.name=gb_goods_info;
set mapred.job.queue.name=root.ai.offline;
SET mapred.max.split.size=128000000;
SET mapred.min.split.size=32000000;
SET mapred.min.split.size.per.node=32000000;
SET mapred.min.split.size.per.rack=32000000;
SET hive.exec.reducers.bytes.per.reducer = 128000000;
SET hive.merge.mapfiles=true;
SET hive.merge.mapredfiles= true;
SET hive.input.format=org.apache.hadoop.hive.ql.io.CombineHiveInputFormat;
SET hive.merge.size.per.task=256000000;
SET hive.exec.parallel = true; 


INSERT OVERWRITE TABLE dw_proj.goods_info 
SELECT
	a.good_sn,
	a.goods_status,
	a.v_wh_code,
	b.cat_name,
	a.cat_id,
	a.good_title,
	a.img_url,
	a.grid_url,
	a.thumb_url,
	a.thumb_extend_url,
	a.stock_qty,
	a.pipeline_code,
	a.url_title
FROM
	(
		SELECT
			good_sn,
			goods_status,
			v_wh_code,
			id AS cat_id,
			good_title,
			img_url,
			grid_url,
			thumb_url,
			thumb_extend_url,
			stock_qty,
			pipeline_code,
			url_title
		FROM
			dw_gearbest_recommend.goods_info_result_uniqlang
		WHERE
			lang = 'en'
		GROUP BY
			good_sn,
			goods_status,
			v_wh_code,
			id,
			good_title,
			img_url,
			grid_url,
			thumb_url,
			thumb_extend_url,
			stock_qty,
			pipeline_code,
			url_title
	) a
JOIN (
	SELECT
		cat_name,
		category_id
	FROM
		stg.gb_goods_site_multi_lang_category
	WHERE
		lang = 'en'
) b ON a.cat_id = b.category_id
;