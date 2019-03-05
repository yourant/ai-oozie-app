--@author ZhanRui
--@date 2019年03月0日 
--@desc  gb邮件推荐BTS算法数据关联商品信息

SET mapred.job.name=apl_result_gb_systm_email_bts1_fact;
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


INSERT OVERWRITE TABLE dw_gearbest_recommend.apl_result_gb_systm_email_bts1_fact
SELECT
	t1.goods_sn1 AS good_sn1,
	t1.goods_sn2 AS good_sn2,
	t3.pipeline_code AS pipeline_code,
	t3.goods_web_sku AS webgoodsn,
	t3.good_title AS goodstitle,
	t3.lang AS lang,
	t3.v_wh_code AS warecode,
	t3.total_num AS reviewcount,
	t3.avg_score AS avgrate,
	t3.shop_price AS shopprice,
	t3.total_favorite AS favoritecount,
	t3.stock_qty AS goodsnum,
	t3.img_url AS imgurl,
	t3.grid_url AS gridurl,
	t3.thumb_url AS thumburl,
	t3.thumb_extend_url AS thumbextendurl,
	t3.url_title AS url_title,
	t1.score AS score
FROM
	(
		SELECT
			goods_sn1,
			goods_sn2,
			score
		FROM
			dw_gearbest_recommend.result_gb_sku_remove_cate
	) t1
JOIN dw_gearbest_recommend.goods_info_result_uniqlang t3 ON t1.goods_sn2 = t3.good_sn
;