--@author Xiongjun1
--@date 2019年03月15日 
--@desc  gb推荐后台后补数据，给商详页第二推荐位使用，
--按pipeline_lang_categoryid分组获取150个后补商品
--按pipeline_lang分组获取500个后补商品

SET mapred.job.name=goods_backup_result_2;
SET mapred.max.split.size=128000000;
SET mapred.min.split.size=32000000;
SET mapred.min.split.size.per.node=32000000;
SET mapred.min.split.size.per.rack=32000000;
SET hive.exec.reducers.bytes.per.reducer = 128000000;
SET hive.merge.mapfiles=true;
SET hive.merge.mapredfiles= true;
SET hive.merge.size.per.task=256000000; 
SET hive.auto.convert.join=false;

 --每个pipeline_code、lang、分类下随机取150个商品
 INSERT overwrite TABLE  dw_gearbest_recommend.goods_info_result_backup_detail2_ctg 
 SELECT
	m.good_sn,
	m.goods_spu,
	m.goods_web_sku,
	m.shop_code,
	m.goods_status,
	m.brand_code,
	m.first_up_time,
	m.v_wh_code,
	m.shop_price,
	--m.id,
	m.level_cnt,
	-- m.level_1,
	-- m.level_2,
	-- m.level_3,
	-- m.level_4,
	m.category_id,
	m.good_title,
	m.img_url,
	m.grid_url,
	m.thumb_url,
	m.thumb_extend_url,
	m.lang,
	m.stock_qty,
	m.avg_score,
	m.total_num,
	m.total_favorite,
	m.pipeline_code,
	m.url_title
FROM
	(
		SELECT
			good_sn,
			goods_spu,
			goods_web_sku,
			shop_code,
			goods_status,
			brand_code,
			first_up_time,
			v_wh_code,
			shop_price,
			--id,
			level_cnt,
			level_1 as category_id,
			--level_2,
			--level_3,
			--level_4,
			good_title,
			img_url,
			grid_url,
			thumb_url,
			thumb_extend_url,
			lang,
			stock_qty,
			avg_score,
			total_num,
			total_favorite,
			pipeline_code,
			url_title,
			ROW_NUMBER () OVER (
				PARTITION BY pipeline_code,
				lang,
				level_1
			ORDER BY
				rand()
			) AS flag
		FROM
			dw_gearbest_recommend.goods_info_result_uniqlang_filtered
		WHERE
			pipeline_code IS NOT NULL
		AND lang IS NOT NULL
		AND level_1 IS NOT NULL
	) m
WHERE
	m.flag <= 150
UNION ALL
SELECT
	m.good_sn,
	m.goods_spu,
	m.goods_web_sku,
	m.shop_code,
	m.goods_status,
	m.brand_code,
	m.first_up_time,
	m.v_wh_code,
	m.shop_price,
	--m.id,
	m.level_cnt,
	-- m.level_1,
	-- m.level_2,
	-- m.level_3,
	-- m.level_4,
	m.category_id,
	m.good_title,
	m.img_url,
	m.grid_url,
	m.thumb_url,
	m.thumb_extend_url,
	m.lang,
	m.stock_qty,
	m.avg_score,
	m.total_num,
	m.total_favorite,
	m.pipeline_code,
	m.url_title
FROM
	(
		SELECT
			good_sn,
			goods_spu,
			goods_web_sku,
			shop_code,
			goods_status,
			brand_code,
			first_up_time,
			v_wh_code,
			shop_price,
			--id,
			level_cnt,
			level_2 as category_id,
			-- level_2,
			-- level_3,
			-- level_4,
			good_title,
			img_url,
			grid_url,
			thumb_url,
			thumb_extend_url,
			lang,
			stock_qty,
			avg_score,
			total_num,
			total_favorite,
			pipeline_code,
			url_title,
			ROW_NUMBER () OVER (
				PARTITION BY pipeline_code,
				lang,
				level_2
			ORDER BY
				rand()
			) AS flag
		FROM
			dw_gearbest_recommend.goods_info_result_uniqlang_filtered
		WHERE
			pipeline_code IS NOT NULL
		AND lang IS NOT NULL
		AND level_2 IS NOT NULL
	) m
WHERE
	m.flag <= 150
UNION ALL
SELECT
	m.good_sn,
	m.goods_spu,
	m.goods_web_sku,
	m.shop_code,
	m.goods_status,
	m.brand_code,
	m.first_up_time,
	m.v_wh_code,
	m.shop_price,
	--m.id,
	m.level_cnt,
	-- m.level_1,
	-- m.level_2,
	-- m.level_3,
	-- m.level_4,
	m.category_id,
	m.good_title,
	m.img_url,
	m.grid_url,
	m.thumb_url,
	m.thumb_extend_url,
	m.lang,
	m.stock_qty,
	m.avg_score,
	m.total_num,
	m.total_favorite,
	m.pipeline_code,
	m.url_title
FROM
	(
		SELECT
			good_sn,
			goods_spu,
			goods_web_sku,
			shop_code,
			goods_status,
			brand_code,
			first_up_time,
			v_wh_code,
			shop_price,
			-- id,
			level_cnt,
			-- level_1,
			-- level_2,
			level_3 as category_id,
			-- level_4,
			good_title,
			img_url,
			grid_url,
			thumb_url,
			thumb_extend_url,
			lang,
			stock_qty,
			avg_score,
			total_num,
			total_favorite,
			pipeline_code,
			url_title,
			ROW_NUMBER () OVER (
				PARTITION BY pipeline_code,
				lang,
				level_3
			ORDER BY
				rand()
			) AS flag
		FROM
			dw_gearbest_recommend.goods_info_result_uniqlang_filtered
		WHERE
			pipeline_code IS NOT NULL
		AND lang IS NOT NULL
		AND level_3 IS NOT NULL
	) m
WHERE
	m.flag <= 150
UNION ALL
SELECT
	m.good_sn,
	m.goods_spu,
	m.goods_web_sku,
	m.shop_code,
	m.goods_status,
	m.brand_code,
	m.first_up_time,
	m.v_wh_code,
	m.shop_price,
	--m.id,
	m.level_cnt,
	-- m.level_1,
	-- m.level_2,
	-- m.level_3,
	-- m.level_4,
	m.category_id,
	m.good_title,
	m.img_url,
	m.grid_url,
	m.thumb_url,
	m.thumb_extend_url,
	m.lang,
	m.stock_qty,
	m.avg_score,
	m.total_num,
	m.total_favorite,
	m.pipeline_code,
	m.url_title
FROM
	(
		SELECT
			good_sn,
			goods_spu,
			goods_web_sku,
			shop_code,
			goods_status,
			brand_code,
			first_up_time,
			v_wh_code,
			shop_price,
			-- id,
			level_cnt,
			-- level_1,
			-- level_2,
			-- level_3,
			level_4 as category_id,
			good_title,
			img_url,
			grid_url,
			thumb_url,
			thumb_extend_url,
			lang,
			stock_qty,
			avg_score,
			total_num,
			total_favorite,
			pipeline_code,
			url_title,
			ROW_NUMBER () OVER (
				PARTITION BY pipeline_code,
				lang,
				level_4
			ORDER BY
				rand()
			) AS flag
		FROM
			dw_gearbest_recommend.goods_info_result_uniqlang_filtered
		WHERE
			pipeline_code IS NOT NULL
		AND lang IS NOT NULL
		AND level_4 IS NOT NULL
	) m
WHERE
	m.flag <= 150;

--相同spu只能取一个sku
INSERT overwrite TABLE dw_gearbest_recommend.goods_info_result_backup_detail2_ctg SELECT
	collect_set(m.good_sn)[0] as good_sn,
	m.goods_spu,
	collect_set(m.goods_web_sku)[0] as goods_web_sku,
	collect_set(m.shop_code)[0] as shop_code,
	collect_set(m.goods_status)[0] as goods_status,
	collect_set(m.brand_code)[0] as brand_code,
	collect_set(m.first_up_time)[0] as first_up_time,
	collect_set(m.v_wh_code)[0] as v_wh_code,
	collect_set(m.shop_price)[0] as shop_price,
	collect_set(m.level_cnt)[0] as level_cnt,
	m.category_id,
	collect_set(m.good_title)[0] as good_title,
	collect_set(m.img_url)[0] as img_url,
	collect_set(m.grid_url)[0] as grid_url,
	collect_set(m.thumb_url)[0] as thumb_url,
	collect_set(m.thumb_extend_url)[0] as thumb_extend_url,
	m.lang,
	collect_set(m.stock_qty)[0] as stock_qty,
	collect_set(m.avg_score)[0] as avg_score,
	collect_set(m.total_num)[0] as total_num,
	collect_set(m.total_favorite)[0] as total_favorite,
	m.pipeline_code,
	collect_set(m.url_title)[0] as url_title
FROM
dw_gearbest_recommend.goods_info_result_backup_detail2_ctg m
GROUP BY
m.pipeline_code,
m.lang,
m.category_id,
m.goods_spu;

	
--最终结果汇总,准备写入Redis的数据结果，过滤网采，过滤推荐位1的sku
INSERT overwrite TABLE dw_gearbest_recommend.goods_backup_result_detail2_ctg SELECT
	t1.good_sn,
	t3.goods_web_sku,
	t1.pipeline_code,
	t3.good_title,
	t1.lang,
	t1.category_id,
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
	t3.url_title
FROM
	dw_gearbest_recommend.goods_info_result_backup_detail2_ctg t1
JOIN 
( 
	SELECT n.* FROM
	dw_gearbest_recommend.goods_info_result_uniqlang n 
	LEFT OUTER JOIN
	(
		SELECT
			a.good_sn
		FROM
			ods.ods_m_gearbest_base_goods_goods a
		WHERE
			a.recommended_level = 14
	) x
	ON n.good_sn = x.good_sn
	WHERE
	x.good_sn is null
) t3
ON t1.good_sn = t3.good_sn 
AND t1.pipeline_code = t3.pipeline_code 
AND t1.lang = t3.lang 
LEFT OUTER JOIN 
(SELECT r.good_sn,
	r.pipeline_code,
	r.lang,
	r.categoryid,
	m.goods_spu
	FROM 
		dw_gearbest_recommend.goods_backup_result_one r
	JOIN 
		dw_gearbest_recommend.goods_info_mid5 m
	ON r.good_sn = m.good_sn
) t4 
ON t1.goods_spu = t4.goods_spu 
AND t1.pipeline_code = t4.pipeline_code 
AND t1.lang = t4.lang 
AND t1.category_id = t4.categoryid 
LEFT OUTER JOIN 
(SELECT r.good_sn,
	r.pipeline_code,
	r.lang,
	m.goods_spu
	FROM 
		dw_gearbest_recommend.goods_backup_result_one_nocategoryid r
	JOIN 
		dw_gearbest_recommend.goods_info_mid5 m
	ON r.good_sn = m.good_sn
) t5 
ON t1.goods_spu = t5.goods_spu 
AND t1.pipeline_code = t5.pipeline_code 
AND t1.lang = t5.lang 
WHERE t4.goods_spu is null 
AND t5.goods_spu is null
;


--每个pipeline_code、lang下随机取50个商品，无分类
 INSERT overwrite TABLE dw_gearbest_recommend.goods_info_result_backup_detail2
 SELECT
	m.good_sn,
	m.goods_spu,
	m.goods_web_sku,
	m.shop_code,
	m.goods_status,
	m.brand_code,
	m.first_up_time,
	m.v_wh_code,
	m.shop_price,
	--m.id,
	m.level_cnt,
	-- m.level_1,
	-- m.level_2,
	-- m.level_3,
	-- m.level_4,
	--m.category_id,
	m.good_title,
	m.img_url,
	m.grid_url,
	m.thumb_url,
	m.thumb_extend_url,
	m.lang,
	m.stock_qty,
	m.avg_score,
	m.total_num,
	m.total_favorite,
	m.pipeline_code,
	m.url_title
FROM
	(
		SELECT
			good_sn,
			goods_spu,
			goods_web_sku,
			shop_code,
			goods_status,
			brand_code,
			first_up_time,
			v_wh_code,
			shop_price,
			-- id,
			level_cnt,
			-- level_1,
			-- level_2,
			-- level_3,
			--level_4 as category_id,
			good_title,
			img_url,
			grid_url,
			thumb_url,
			thumb_extend_url,
			lang,
			stock_qty,
			avg_score,
			total_num,
			total_favorite,
			pipeline_code,
			url_title,
			ROW_NUMBER () OVER (
				PARTITION BY pipeline_code,
				lang
			ORDER BY
				rand()
			) AS flag
		FROM
			dw_gearbest_recommend.goods_info_result_uniqlang_filtered
		WHERE
			pipeline_code IS NOT NULL
		AND lang IS NOT NULL
	) m
WHERE
	m.flag <= 500;

--相同spu只能取一个sku
INSERT overwrite TABLE dw_gearbest_recommend.goods_info_result_backup_detail2 SELECT
	collect_set(m.good_sn)[0] as good_sn,
	m.goods_spu,
	collect_set(m.goods_web_sku)[0] as goods_web_sku,
	collect_set(m.shop_code)[0] as shop_code,
	collect_set(m.goods_status)[0] as goods_status,
	collect_set(m.brand_code)[0] as brand_code,
	collect_set(m.first_up_time)[0] as first_up_time,
	collect_set(m.v_wh_code)[0] as v_wh_code,
	collect_set(m.shop_price)[0] as shop_price,
	collect_set(m.level_cnt)[0] as level_cnt,
	collect_set(m.good_title)[0] as good_title,
	collect_set(m.img_url)[0] as img_url,
	collect_set(m.grid_url)[0] as grid_url,
	collect_set(m.thumb_url)[0] as thumb_url,
	collect_set(m.thumb_extend_url)[0] as thumb_extend_url,
	m.lang,
	collect_set(m.stock_qty)[0] as stock_qty,
	collect_set(m.avg_score)[0] as avg_score,
	collect_set(m.total_num)[0] as total_num,
	collect_set(m.total_favorite)[0] as total_favorite,
	m.pipeline_code,
	collect_set(m.url_title)[0] as url_title
FROM
dw_gearbest_recommend.goods_info_result_backup_detail2 m
GROUP BY
m.pipeline_code,
m.lang,
m.goods_spu;

                  
--最终结果汇总,准备写入Redis的数据结果，过滤网采，过滤分类下的，过滤推荐位1的
INSERT overwrite TABLE dw_gearbest_recommend.goods_backup_result_detail2 SELECT
	t1.good_sn,
	t3.goods_web_sku,
	t1.pipeline_code,
	t3.good_title,
	t1.lang,
	0,
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
	t3.url_title
FROM
	dw_gearbest_recommend.goods_info_result_backup_detail2 t1
JOIN 
( 
	SELECT n.* FROM
	dw_gearbest_recommend.goods_info_result_uniqlang n 
	LEFT OUTER JOIN
	(
		SELECT
			a.good_sn
		FROM
			ods.ods_m_gearbest_base_goods_goods a
		WHERE
			a.recommended_level = 14
	) x
	ON n.good_sn = x.good_sn
	WHERE
	x.good_sn is null
) t3
ON t1.good_sn = t3.good_sn
AND t1.pipeline_code = t3.pipeline_code
AND t1.lang = t3.lang
LEFT OUTER JOIN 
(SELECT r.good_sn,
	r.pipeline_code,
	r.lang,
	m.goods_spu
	FROM 
		dw_gearbest_recommend.goods_backup_result_one r
	JOIN 
		dw_gearbest_recommend.goods_info_mid5 m
	ON r.good_sn = m.good_sn
) t4  
ON t1.goods_spu = t4.goods_spu 
AND t1.pipeline_code = t4.pipeline_code 
AND t1.lang = t4.lang 
-- LEFT OUTER JOIN dw_gearbest_recommend.goods_info_result_backup_detail2_ctg t5 
-- ON t1.goods_spu = t5.goods_spu 
-- AND t1.pipeline_code = t5.pipeline_code 
-- AND t1.lang = t5.lang 
--WHERE t5.goods_spu is null
LEFT OUTER JOIN 
(SELECT r.good_sn,
	r.pipeline_code,
	r.lang,
	m.goods_spu
	FROM 
		dw_gearbest_recommend.goods_backup_result_one_nocategoryid r
	JOIN 
		dw_gearbest_recommend.goods_info_mid5 m
	ON r.good_sn = m.good_sn
) t5 
ON t1.goods_spu = t5.goods_spu 
AND t1.pipeline_code = t5.pipeline_code 
AND t1.lang = t5.lang 
WHERE t4.goods_spu is null
AND t5.goods_spu is null
;