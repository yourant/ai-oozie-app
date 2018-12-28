--@author ZhanRui
--@date 2018年10月19日 
--@desc  gb推荐后台网采商品过滤信息表

SET mapred.job.name=goods_bf_result;
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

--网采商品禁售国家
INSERT OVERWRITE TABLE dw_gearbest_recommend.bf_pipline_country SELECT
	'key' AS bf_country_key,
	x.country_code AS bf_country_value
FROM
	(
		SELECT
			pipeline_code,
			country_code
		FROM
			ods.ods_m_gearbest_3w_logts_warehouse_forbid_country_rel
		WHERE
			forbid_attr_id = 107
		GROUP BY
			pipeline_code,
			country_code
	) x
GROUP BY
	x.country_code
    ;





--所有允许卖的sku，非禁售sku统计
INSERT overwrite TABLE dw_gearbest_recommend.bf_good_sn  SELECT
    'key' AS bf_good_sn_key,
	n.good_sn AS bf_good_sn_value
FROM
	dw_gearbest_recommend.goods_info_result_uniq n
WHERE
	n.good_sn NOT IN (
		SELECT
			x.good_sn
		FROM
			(
				SELECT
					a.good_sn
				FROM
					ods.ods_m_gearbest_base_goods_goods a
				WHERE
					a.recommended_level = 14
			) x
	)
GROUP BY n.good_sn
;
