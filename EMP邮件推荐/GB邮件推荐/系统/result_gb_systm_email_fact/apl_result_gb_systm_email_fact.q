--@author ZhanRui
--@date 2018年11月06日 
--@desc  gb邮件推荐算法数据关联商品信息

SET mapred.job.name=apl_result_gb_systm_email_fact;
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

INSERT OVERWRITE TABLE dw_gearbest_recommend.apl_result_gb_systm_email_fact SELECT
	NVL (user_id, '') as user_id,
	NVL (good_sn, '') as good_sn,
	NVL (pipeline_code, '') as pipeline_code,
	NVL (goods_web_sku, '') AS webgoodsn,
	NVL (good_title, '') AS goodstitle,
	NVL (lang, '') as lang,
	NVL (shop_code, 0) AS shopcode,
	NVL (id, 0) AS catid,
	NVL (v_wh_code, 0) AS warecode,
	NVL (total_num, 0) AS reviewcount,
	NVL (avg_score, 0) AS avgrate,
	NVL (shop_price, 0) AS shopprice,
	NVL (total_favorite, 0) AS favoritecount,
	NVL (stock_qty, 0) AS goodsnum,
	NVL (img_url, '') AS imgurl,
	NVL (grid_url, '') AS gridurl,
	NVL (thumb_url, '') AS thumburl,
	NVL (thumb_extend_url, '') AS thumbextendurl,
	NVL (url_title, '') as url_title,
	NVL (score, 0) AS score
FROM
	(
		SELECT
			t1.user_id,
			t1.good_sn,
			t3.pipeline_code,
			t3.goods_web_sku,
			t3.good_title,
			t3.lang,
			t3.shop_code,
			t3.id,
			t3.v_wh_code,
			t3.total_num,
			t3.avg_score,
			t3.shop_price,
			t3.total_favorite,
			t3.stock_qty,
			t3.img_url,
			t3.grid_url,
			t3.thumb_url,
			t3.thumb_extend_url,
			t3.url_title,
			t1.score
		FROM
			(
				SELECT
					user_id,
					good_sn,
					score
				FROM
					dw_gearbest_recommend.result_gb_systm_email_gtq
				WHERE
					 concat_ws('-', year, month, day) = '${ADD_TIME}'
			) t1
		JOIN dw_gearbest_recommend.goods_info_result_uniqlang t3 ON t1.good_sn = t3.good_sn
	) tmp;