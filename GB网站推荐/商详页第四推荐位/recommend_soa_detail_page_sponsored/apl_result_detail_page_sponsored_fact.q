--@author ZhanRui
--@date 2018年10月23日 
--@desc  gb商详页第四推荐位后台数据

SET mapred.job.name=recommend_soa_detail_page_sponsored;
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
SET hive.auto.convert.join=false;--关闭优化 20190415 zhangyuchao



--过滤算法sku
DROP TABLE IF EXISTS dw_gearbest_recommend.tmp_gb_result_detail_page_skus_4detail;
CREATE TABLE dw_gearbest_recommend.tmp_gb_result_detail_page_skus_4detail as
	select DISTINCT goods_sn2 as good_sn
		from dw_gearbest_recommend.gb_result_detail_page_gtq 
		where concat(year, month, day)=${ADD_TIME}
	UNION ALL
	select DISTINCT goods_sn2 as good_sn
		from dw_gearbest_recommend.gb_result_detail_1_page_gtq 
		where concat(year, month, day)=${ADD_TIME}
	UNION ALL
	select DISTINCT good_sn
		from dw_gearbest_recommend.apl_lable_new_fact
	UNION ALL
	select DISTINCT good_sn
		from dw_gearbest_recommend.apl_lable_money_fact
		;


--DA商品池
INSERT overwrite TABLE dw_gearbest_recommend.goods_info_result_uniqlang_da SELECT
	n.*
FROM
	dw_gearbest_recommend.goods_info_result_uniqlang n
WHERE
	n.good_sn IN (
		--distinct sku--20190409-zhangyuchao
		SELECT DISTINCT
			a.good_sn
		FROM
			ods.ods_m_gearbest_base_goods_new_goods_label a
		-- left join
		-- 	(select DISTINCT goods_sn2 
		-- 	from dw_gearbest_recommend.gb_result_detail_page_gtq 
		-- 	where concat(year, month, day)=${ADD_TIME} ) b
		-- on 
		--    a.good_sn = b.goods_sn2 
		-- left join 
		-- 	(select DISTINCT goods_sn2 
		-- 	from dw_gearbest_recommend.gb_result_detail_1_page_gtq 
		-- 	where concat(year, month, day)=${ADD_TIME}) c
		-- on a.good_sn = c.goods_sn2
		-- left join 
		-- 	(select distinct good_sn 
		-- 	from dw_gearbest_recommend.apl_lable_new_fact) d
		-- on a.good_sn = d.good_sn
		-- left join 
		-- 	(select distinct good_sn 
		-- 	from dw_gearbest_recommend.apl_lable_money_fact) e
		-- on a.good_sn = e.good_sn
		left join
			dw_gearbest_recommend.tmp_gb_result_detail_page_skus_4detail b
		on 
			a.good_sn = b.good_sn 
		where 
			a.label_code = '00000238'
			and b.good_sn is null
	);


--BF出单商品池
INSERT overwrite TABLE dw_gearbest_recommend.goods_info_result_uniqlang_bf SELECT
	n.*
FROM
	dw_gearbest_recommend.goods_info_result_uniqlang n
WHERE
	n.good_sn IN (
		SELECT DISTINCT
			x.good_sn
		FROM
			(
				--distinct sku--20190409-zhangyuchao
				SELECT DISTINCT
					a.good_sn
				FROM
					ods.ods_m_gearbest_base_goods_goods a
				-- left join
				-- 	(select DISTINCT goods_sn2 
				-- 	from dw_gearbest_recommend.gb_result_detail_page_gtq 
				-- 	where concat(year, month, day)=${ADD_TIME}) b
				-- on 
				-- 	a.good_sn = b.goods_sn2 
				-- left join 
				-- 	(select DISTINCT goods_sn2 
				-- 	from dw_gearbest_recommend.gb_result_detail_1_page_gtq 
				-- 	where concat(year, month, day)=${ADD_TIME}) c
				-- on a.good_sn = c.goods_sn2 
				-- left join 
				-- 	(select distinct good_sn 
				-- 	from dw_gearbest_recommend.apl_lable_new_fact) d
				-- on a.good_sn = d.good_sn
				-- left join 
				-- 	(select distinct good_sn 
				-- 	from dw_gearbest_recommend.apl_lable_money_fact) e
				-- on a.good_sn = e.good_sn				
				left join
					dw_gearbest_recommend.tmp_gb_result_detail_page_skus_4detail b
				on 
					a.good_sn = b.good_sn 
				where 
					a.recommended_level = 14
				and b.good_sn is null
				-- and c.goods_sn2 is null	
				-- and d.good_sn is null
				-- and e.good_sn is null
			) x
		JOIN stg.gb_order_order_goods m ON x.good_sn = m.goods_sn
	);



 --DA基础数据准备，先随机取商品池每个pipeline_code、lang、id下的50个商品
 INSERT overwrite TABLE dw_gearbest_recommend.goods_info_result_da  SELECT
	m.good_sn,
	m.goods_spu,
	m.goods_web_sku,
	m.shop_code,
	m.goods_status,
	m.brand_code,
	m.first_up_time,
	m.v_wh_code,
	m.shop_price,
	m.id,
	m.level_cnt,
	m.level_1,
	m.level_2,
	m.level_3,
	m.level_4,
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
			id,
			level_cnt,
			level_1,
			level_2,
			level_3,
			level_4,
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
				id
			ORDER BY
				rand()
			) AS flag
		FROM
			dw_gearbest_recommend.goods_info_result_uniqlang_da
		WHERE
			pipeline_code IS NOT NULL
		AND lang IS NOT NULL
		AND id IS NOT NULL
	) m
WHERE
	m.flag <= 50;




--DA各分类数据统计，商品池的商品是按照底级分类存的，向上构造父分类商品池
INSERT overwrite TABLE dw_gearbest_recommend.goods_da_tmp  SELECT
	m.pipeline_code,
	m.goods_spu,
	m.lang,
	m.category_id
FROM
	(
		SELECT
			a.pipeline_code,
			a.goods_spu,
			a.lang,
			a.level_1 AS category_id
		FROM
			dw_gearbest_recommend.goods_info_result_da a
		WHERE
			level_1 IS NOT NULL
		GROUP BY
			a.pipeline_code,
			a.goods_spu,
			a.lang,
			a.level_1
		UNION ALL
			SELECT
				a.pipeline_code,
				a.goods_spu,
				a.lang,
				a.level_2 AS category_id
			FROM
				dw_gearbest_recommend.goods_info_result_da a
			WHERE
				level_2 IS NOT NULL
			GROUP BY
				a.pipeline_code,
				a.goods_spu,
				a.lang,
				a.level_2
			UNION ALL
				SELECT
					a.pipeline_code,
					a.goods_spu,
					a.lang,
					a.level_3 AS category_id
				FROM
					dw_gearbest_recommend.goods_info_result_da a
				WHERE
					level_3 IS NOT NULL
				GROUP BY
					a.pipeline_code,
					a.goods_spu,
					a.lang,
					a.level_3
				UNION ALL
					SELECT
						a.pipeline_code,
						a.goods_spu,
						a.lang,
						a.level_4 AS category_id
					FROM
						dw_gearbest_recommend.goods_info_result_da a
					WHERE
						level_4 IS NOT NULL
					GROUP BY
						a.pipeline_code,
						a.goods_spu,
						a.lang,
						a.level_4
	) m
GROUP BY
	m.pipeline_code,
	m.goods_spu,
	m.lang,
	m.category_id
;


--缩小数据量，每个分类取50个数据
INSERT overwrite TABLE dw_gearbest_recommend.goods_da_tmp1  SELECT
	m.pipeline_code,
	m.goods_spu,
	m.lang,
	m.category_id
FROM
	(
		SELECT
			pipeline_code,
			goods_spu,
			lang,
			category_id,
			ROW_NUMBER () OVER (
				PARTITION BY pipeline_code,
				lang,
				category_id
			ORDER BY
				rand()
			) AS flag
		FROM
			dw_gearbest_recommend.goods_da_tmp
	) m
WHERE
	m.flag <= 50
;

--取3级分类 来补分类不够50个的情况
INSERT OVERWRITE TABLE dw_gearbest_recommend.goods_da_autocomplete  SELECT
	m.pipeline_code,
	n.goods_spu,
	m.lang,
	m.category_id
FROM
	(
		SELECT
			a.pipeline_code,
			a.category_id,
			a.lang,
			b.level_3
		FROM
			(
				SELECT
					pipeline_code,
					category_id,
					lang,
					COUNT(goods_spu) cnt
				FROM
					dw_gearbest_recommend.goods_da_tmp1
				GROUP BY
					pipeline_code,
					category_id,
					lang
				HAVING
					cnt < 50
			) a
		JOIN dw_gearbest_recommend.goods_category_level b ON a.category_id = b.id
	) m
JOIN dw_gearbest_recommend.goods_da_tmp1 n ON m.pipeline_code = n.pipeline_code
AND m.level_3 = n.category_id
AND m.lang = n.lang
UNION ALL
	SELECT
		x.pipeline_code,
		x.goods_spu,
		x.lang,
		x.category_id
	FROM
		dw_gearbest_recommend.goods_da_tmp1 x
;

--取2级分类 来补分类不够50个的情况
INSERT OVERWRITE TABLE dw_gearbest_recommend.goods_da_autocomplete  SELECT
	m.pipeline_code,
	n.goods_spu,
	m.lang,
	m.category_id
FROM
	(
		SELECT
			a.pipeline_code,
			a.category_id,
			a.lang,
			b.level_2
		FROM
			(
				SELECT
					pipeline_code,
					category_id,
					lang,
					COUNT(goods_spu) cnt
				FROM
					dw_gearbest_recommend.goods_da_autocomplete
				GROUP BY
					pipeline_code,
					category_id,
					lang
				HAVING
					cnt < 50
			) a
		JOIN dw_gearbest_recommend.goods_category_level b ON a.category_id = b.id
	) m
JOIN dw_gearbest_recommend.goods_da_autocomplete n ON m.pipeline_code = n.pipeline_code
AND m.level_2 = n.category_id
AND m.lang = n.lang
UNION ALL
	SELECT
		x.pipeline_code,
		x.goods_spu,
		x.lang,
		x.category_id
	FROM
		dw_gearbest_recommend.goods_da_autocomplete x
;

--取1级分类 来补分类不够50个的情况
INSERT OVERWRITE TABLE dw_gearbest_recommend.goods_da_autocomplete  SELECT
	m.pipeline_code,
	n.goods_spu,
	m.lang,
	m.category_id
FROM
	(
		SELECT
			a.pipeline_code,
			a.category_id,
			a.lang,
			b.level_1
		FROM
			(
				SELECT
					pipeline_code,
					category_id,
					lang,
					COUNT(goods_spu) cnt
				FROM
					dw_gearbest_recommend.goods_da_autocomplete
				GROUP BY
					pipeline_code,
					category_id,
					lang
				HAVING
					cnt < 50
			) a
		JOIN dw_gearbest_recommend.goods_category_level b ON a.category_id = b.id
	) m
JOIN dw_gearbest_recommend.goods_da_autocomplete n ON m.pipeline_code = n.pipeline_code
AND m.level_1 = n.category_id
AND m.lang = n.lang
UNION ALL
	SELECT
		x.pipeline_code,
		x.goods_spu,
		x.lang,
		x.category_id
	FROM
		dw_gearbest_recommend.goods_da_autocomplete x
;

--BF基础数据准备，先随机取商品池每个pipeline_code、lang、id下的50个商品
 INSERT overwrite TABLE dw_gearbest_recommend.goods_info_result_bf  SELECT
	m.good_sn,
	m.goods_spu,
	m.goods_web_sku,
	m.shop_code,
	m.goods_status,
	m.brand_code,
	m.first_up_time,
	m.v_wh_code,
	m.shop_price,
	m.id,
	m.level_cnt,
	m.level_1,
	m.level_2,
	m.level_3,
	m.level_4,
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
			id,
			level_cnt,
			level_1,
			level_2,
			level_3,
			level_4,
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
				id
			ORDER BY
				rand()
			) AS flag
		FROM
			dw_gearbest_recommend.goods_info_result_uniqlang_bf
		WHERE
			pipeline_code IS NOT NULL
		AND lang IS NOT NULL
		AND id IS NOT NULL
	) m
WHERE
	m.flag <= 50;


--BF各分类数据统计，商品池的商品是按照底级分类存的，向上构造父分类商品池
INSERT overwrite TABLE dw_gearbest_recommend.goods_bf_tmp  SELECT
	m.pipeline_code,
	m.goods_spu,
	m.lang,
	m.category_id
FROM
	(
		SELECT
			a.pipeline_code,
			a.goods_spu,
			a.lang,
			a.level_1 AS category_id
		FROM
			dw_gearbest_recommend.goods_info_result_bf a
		WHERE
			level_1 IS NOT NULL
		GROUP BY
			a.pipeline_code,
			a.goods_spu,
			a.lang,
			a.level_1
		UNION ALL
			SELECT
				a.pipeline_code,
				a.goods_spu,
				a.lang,
				a.level_2 AS category_id
			FROM
				dw_gearbest_recommend.goods_info_result_bf a
			WHERE
				level_2 IS NOT NULL
			GROUP BY
				a.pipeline_code,
				a.goods_spu,
				a.lang,
				a.level_2
			UNION ALL
				SELECT
					a.pipeline_code,
					a.goods_spu,
					a.lang,
					a.level_3 AS category_id
				FROM
					dw_gearbest_recommend.goods_info_result_bf a
				WHERE
					level_3 IS NOT NULL
				GROUP BY
					a.pipeline_code,
					a.goods_spu,
					a.lang,
					a.level_3
				UNION ALL
					SELECT
						a.pipeline_code,
						a.goods_spu,
						a.lang,
						a.level_4 AS category_id
					FROM
						dw_gearbest_recommend.goods_info_result_bf a
					WHERE
						level_4 IS NOT NULL
					GROUP BY
						a.pipeline_code,
						a.goods_spu,
						a.lang,
						a.level_4
	) m
GROUP BY
	m.pipeline_code,
	m.goods_spu,
	m.lang,
	m.category_id
;


--缩小数据量，每个分类取50个数据
INSERT overwrite TABLE dw_gearbest_recommend.goods_bf_tmp1  SELECT
	m.pipeline_code,
	m.goods_spu,
	m.lang,
	m.category_id
FROM
	(
		SELECT
			pipeline_code,
			goods_spu,
			lang,
			category_id,
			ROW_NUMBER () OVER (
				PARTITION BY pipeline_code,
				lang,
				category_id
			ORDER BY
				rand()
			) AS flag
		FROM
			dw_gearbest_recommend.goods_bf_tmp
	) m
WHERE
	m.flag <= 50
;

--取3级分类 来补分类不够50个的情况
INSERT OVERWRITE TABLE dw_gearbest_recommend.goods_bf_autocomplete  SELECT
	m.pipeline_code,
	n.goods_spu,
	m.lang,
	m.category_id
FROM
	(
		SELECT
			a.pipeline_code,
			a.category_id,
			a.lang,
			b.level_3
		FROM
			(
				SELECT
					pipeline_code,
					category_id,
					lang,
					COUNT(goods_spu) cnt
				FROM
					dw_gearbest_recommend.goods_bf_tmp1
				GROUP BY
					pipeline_code,
					category_id,
					lang
				HAVING
					cnt < 50
			) a
		JOIN dw_gearbest_recommend.goods_category_level b ON a.category_id = b.id
	) m
JOIN dw_gearbest_recommend.goods_bf_tmp1 n ON m.pipeline_code = n.pipeline_code
AND m.level_3 = n.category_id
AND m.lang = n.lang
UNION ALL
	SELECT
		x.pipeline_code,
		x.goods_spu,
		x.lang,
		x.category_id
	FROM
		dw_gearbest_recommend.goods_bf_tmp1 x
;

--取2级分类 来补分类不够50个的情况
INSERT OVERWRITE TABLE dw_gearbest_recommend.goods_bf_autocomplete  SELECT
	m.pipeline_code,
	n.goods_spu,
	m.lang,
	m.category_id
FROM
	(
		SELECT
			a.pipeline_code,
			a.category_id,
			a.lang,
			b.level_2
		FROM
			(
				SELECT
					pipeline_code,
					category_id,
					lang,
					COUNT(goods_spu) cnt
				FROM
					dw_gearbest_recommend.goods_bf_autocomplete
				GROUP BY
					pipeline_code,
					category_id,
					lang
				HAVING
					cnt < 50
			) a
		JOIN dw_gearbest_recommend.goods_category_level b ON a.category_id = b.id
	) m
JOIN dw_gearbest_recommend.goods_bf_autocomplete n ON m.pipeline_code = n.pipeline_code
AND m.level_2 = n.category_id
AND m.lang = n.lang
UNION ALL
	SELECT
		x.pipeline_code,
		x.goods_spu,
		x.lang,
		x.category_id
	FROM
		dw_gearbest_recommend.goods_bf_autocomplete x
;

--取1级分类 来补分类不够50个的情况
INSERT OVERWRITE TABLE dw_gearbest_recommend.goods_bf_autocomplete  SELECT
	m.pipeline_code,
	n.goods_spu,
	m.lang,
	m.category_id
FROM
	(
		SELECT
			a.pipeline_code,
			a.category_id,
			a.lang,
			b.level_1
		FROM
			(
				SELECT
					pipeline_code,
					category_id,
					lang,
					COUNT(goods_spu) cnt
				FROM
					dw_gearbest_recommend.goods_bf_autocomplete
				GROUP BY
					pipeline_code,
					category_id,
					lang
				HAVING
					cnt < 50
			) a
		JOIN dw_gearbest_recommend.goods_category_level b ON a.category_id = b.id
	) m
JOIN dw_gearbest_recommend.goods_bf_autocomplete n ON m.pipeline_code = n.pipeline_code
AND m.level_1 = n.category_id
AND m.lang = n.lang
UNION ALL
	SELECT
		x.pipeline_code,
		x.goods_spu,
		x.lang,
		x.category_id
	FROM
		dw_gearbest_recommend.goods_bf_autocomplete x
;



--将DA商品和BF商品数据汇总，缩小数据量，每个分类下取30个，去重
INSERT overwrite TABLE dw_gearbest_recommend.goods_da_bf_merge SELECT
	n.pipeline_code,
	n.goods_spu,
	n.lang,
	n.category_id
FROM
	(
		SELECT
			m.pipeline_code,
			m.goods_spu,
			m.lang,
			m.category_id,
			ROW_NUMBER () OVER (
				PARTITION BY m.pipeline_code,
				m.lang,
				m.category_id
			ORDER BY
				m.rank
			) AS flag
		FROM
			(
				SELECT
					pipeline_code,
					goods_spu,
					lang,
					category_id,
					0 AS rank
				FROM
					dw_gearbest_recommend.goods_da_autocomplete
				UNION ALL
					SELECT
						pipeline_code,
						goods_spu,
						lang,
						category_id,
						1 AS rank
					FROM
						dw_gearbest_recommend.goods_bf_autocomplete
			) m
	) n
WHERE
	n.flag <= 30
group by 
	n.pipeline_code,
	n.goods_spu,
	n.lang,
	n.category_id
;



--最终结果汇总,准备写入Redis的数据结果
INSERT overwrite TABLE dw_gearbest_recommend.apl_result_detail_page_sponsored_fact SELECT
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
	(
		SELECT
			m.pipeline_code,
			m.goods_spu,
			m.lang,
			m.category_id,
			n.good_sn
		FROM
			dw_gearbest_recommend.goods_da_bf_merge m
		JOIN 
		--dw_gearbest_recommend.goods_info_mid5
		( 
			SELECT a.good_sn,a.goods_spu 
			FROM 
				dw_gearbest_recommend.goods_info_mid5 a
			left join 
				dw_gearbest_recommend.tmp_gb_result_detail_page_skus_4detail b
			on 
				a.good_sn = b.good_sn 
		) n ON m.goods_spu = n.goods_spu
	) t1
JOIN dw_gearbest_recommend.goods_info_result_uniqlang t3 ON t1.good_sn = t3.good_sn
AND t1.pipeline_code = t3.pipeline_code
AND t1.lang = t3.lang
;