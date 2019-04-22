--@author ZhanRui
--@date 2018年11月06日 
--@desc  gb邮件推荐算法数据关联商品信息

SET mapred.job.name=apl_result_rg_system_email_rg_cf;
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


INSERT OVERWRITE TABLE dw_rg_recommend.apl_result_rg_system_email_rg_cf SELECT
	NVL (user_id, '') as user_id,
	NVL (goodssn, '') as goodssn,
	NVL (goodsid, 0) as goodsid,
	NVL (catid, 0) as catid,
	NVL (lang, '') AS lang,
	NVL (goodstitle, '') AS goodstitle,
	NVL (gridurl, '') AS gridurl,
	NVL (imgurl, '') AS imgurl,
	NVL (thumburl, '') AS thumburl,
	NVL (urltitle, '') AS urltitle,
	NVL (score, 0) AS score
FROM
	(
		SELECT
			t1.user_id,
			t3.goodssn,
			t3.goodsid,
			t3.catid,
			t3.lang,
			t3.goodstitle,
			t3.gridurl,
			t3.imgurl,
			t3.thumburl,
			t3.urltitle,
			t1.score
		FROM
			(
				SELECT
					user_id,
					good_sn,
					score
				FROM
					dw_rg_recommend.result_rg_system_email_rg_cf
				WHERE
					 concat_ws('-', year, month, day) = '${ADD_TIME}'
			) t1
		JOIN dw_rg_recommend.apl_rg_goods_sn_2hbase_good_link t3 ON t1.good_sn = t3.goodssn
	)  tmp;