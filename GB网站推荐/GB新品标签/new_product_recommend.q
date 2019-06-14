--@author XiongJun
--@date 2019年4月28日 
--@desc  gb新品标签，按照pipelineCode_lang_catid_重复曝光序号_商品序号_sku

SET mapred.job.name=new_product_recommend;
SET mapred.max.split.size=128000000;
SET mapred.min.split.size=32000000;
SET mapred.min.split.size.per.node=32000000;
SET mapred.min.split.size.per.rack=32000000;
SET hive.exec.reducers.bytes.per.reducer = 128000000;
SET hive.merge.mapfiles=true;
SET hive.merge.mapredfiles= true;
SET hive.merge.size.per.task=256000000; 
SET hive.auto.convert.join = false;
use dw_gearbest_recommend;


--获取新品及标签，关联出渠道、语言、分类id
 INSERT overwrite TABLE dw_gearbest_recommend.gb_new_product_result_backup  SELECT
	m.good_sn,
	m.goods_spu,
	m.id,
	m.level_1,
	m.level_2,
	m.level_3,
	m.level_4,
	m.lang,
	m.pipeline_code,
    n.lable_name
FROM	
	dw_gearbest_recommend.goods_info_result_uniqlang_filtered m
JOIN 
    dw_gearbest_recommend.gb_new_product_lable n
ON 
    m.good_sn = n.good_sn
--T+1过滤
WHERE
	m.stock_qty > 0 AND m.goods_status = 2;

--各分类数据统计，取当前分类商品
INSERT overwrite TABLE dw_gearbest_recommend.gb_new_product_backup_current_tmp  SELECT
	m.pipeline_code,
	m.good_sn,
	m.goods_spu,
	m.lang,
	m.category_id,
    m.lable_name,    
    0 as seq
FROM
	(
		SELECT
			a.pipeline_code,
			a.good_sn,
			a.goods_spu,
			a.lang,
			a.level_1 AS category_id,
            a.lable_name
		FROM
			dw_gearbest_recommend.gb_new_product_result_backup a
		WHERE
			level_1 IS NOT NULL AND level_1 = id 
		GROUP BY
			a.pipeline_code,
			a.good_sn,
			a.goods_spu,
			a.lang,
			a.level_1,
            a.lable_name
		UNION ALL
			SELECT
				a.pipeline_code,
				a.good_sn,
				a.goods_spu,
				a.lang,
				a.level_2 AS category_id,
                a.lable_name
			FROM
				dw_gearbest_recommend.gb_new_product_result_backup a
			WHERE
				level_2 IS NOT NULL AND level_2 = id 
			GROUP BY
				a.pipeline_code,
				a.good_sn,
				a.goods_spu,
				a.lang,
				a.level_2,
                a.lable_name
        UNION ALL
            SELECT
                a.pipeline_code,
                a.good_sn,
				a.goods_spu,
                a.lang,
                a.level_3 AS category_id,
                a.lable_name
            FROM
                dw_gearbest_recommend.gb_new_product_result_backup a
            WHERE
                level_3 IS NOT NULL AND level_3 = id 
            GROUP BY
                a.pipeline_code,
                a.good_sn,
				a.goods_spu,
                a.lang,
                a.level_3,
                a.lable_name
        UNION ALL
            SELECT
                a.pipeline_code,
                a.good_sn,
				a.goods_spu,
                a.lang,
                a.level_4 AS category_id,
                a.lable_name
            FROM
                dw_gearbest_recommend.gb_new_product_result_backup a
            WHERE
                level_4 IS NOT NULL AND level_4 = id 
            GROUP BY
                a.pipeline_code,
                a.good_sn,
				a.goods_spu,
                a.lang,
                a.level_4,
                a.lable_name
	) m
GROUP BY
	m.pipeline_code,
	m.good_sn,
	m.goods_spu,
	m.lang,
	m.category_id,
    m.lable_name;

--随机打散
INSERT overwrite TABLE dw_gearbest_recommend.gb_new_product_backup_current_tmp  SELECT
	pipeline_code,
	good_sn,
	goods_spu,
	lang,
	category_id,
    lable_name,
    ROW_NUMBER () OVER (
        ORDER BY
            rand()
        ) AS seq
FROM dw_gearbest_recommend.gb_new_product_backup_current_tmp;

--------------------------------------------------------------------------------------------------

--各分类数据统计，商品池的商品是按照底级分类存的，向上构造父分类商品池
INSERT overwrite TABLE dw_gearbest_recommend.gb_new_product_backup_parent_tmp  SELECT
	m.pipeline_code,
	m.good_sn,
	m.goods_spu,
	m.lang,
	m.category_id,
    m.lable_name,    
    0 as seq
FROM
	(
		SELECT
			a.pipeline_code,
			a.good_sn,
			a.goods_spu,
			a.lang,
			a.level_1 AS category_id,
            a.lable_name
		FROM
			dw_gearbest_recommend.gb_new_product_result_backup a
		WHERE
			level_1 IS NOT NULL
		GROUP BY
			a.pipeline_code,
			a.good_sn,
			a.goods_spu,
			a.lang,
			a.level_1,
            a.lable_name
		UNION ALL
			SELECT
				a.pipeline_code,
				a.good_sn,
				a.goods_spu,
				a.lang,
				a.level_2 AS category_id,
                a.lable_name
			FROM
				dw_gearbest_recommend.gb_new_product_result_backup a
			WHERE
				level_2 IS NOT NULL
			GROUP BY
				a.pipeline_code,
				a.good_sn,
				a.goods_spu,
				a.lang,
				a.level_2,
                a.lable_name
        UNION ALL
            SELECT
                a.pipeline_code,
                a.good_sn,
				a.goods_spu,
                a.lang,
                a.level_3 AS category_id,
                a.lable_name
            FROM
                dw_gearbest_recommend.gb_new_product_result_backup a
            WHERE
                level_3 IS NOT NULL
            GROUP BY
                a.pipeline_code,
                a.good_sn,
				a.goods_spu,
                a.lang,
                a.level_3,
                a.lable_name
        UNION ALL
            SELECT
                a.pipeline_code,
                a.good_sn,
				a.goods_spu,
                a.lang,
                a.level_4 AS category_id,
                a.lable_name
            FROM
                dw_gearbest_recommend.gb_new_product_result_backup a
            WHERE
                level_4 IS NOT NULL
            GROUP BY
                a.pipeline_code,
                a.good_sn,
				a.goods_spu,
                a.lang,
                a.level_4,
                a.lable_name
	) m
GROUP BY
	m.pipeline_code,
	m.good_sn,
	m.goods_spu,
	m.lang,
	m.category_id,
    m.lable_name;

 --取3级分类 来补分类
INSERT OVERWRITE TABLE dw_gearbest_recommend.gb_new_product_backup_parent_tmp  SELECT
	m.pipeline_code,
	n.good_sn,
	n.goods_spu,
	m.lang,
	m.category_id,
    m.lable_name,    
    0 as seq
FROM
	(
		SELECT
			a.pipeline_code,
			a.category_id,
			a.lang,
            a.lable_name,
			b.level_3
		FROM
			(
				SELECT
					pipeline_code,
					category_id,
					lang,
                	lable_name,
					COUNT(goods_spu) cnt
				FROM
					dw_gearbest_recommend.gb_new_product_backup_parent_tmp
				GROUP BY
					pipeline_code,
					category_id,
					lang,
                	lable_name 
				HAVING
					cnt < 500
			) a
		JOIN ods.ods_o_gearbest_bigdata_goods_category_level b ON a.category_id = b.id
		WHERE b.day = '${DATE}'
	) m
JOIN dw_gearbest_recommend.gb_new_product_backup_parent_tmp n ON m.pipeline_code = n.pipeline_code
AND m.level_3 = n.category_id
AND m.lang = n.lang
AND m.lable_name = n.lable_name
UNION ALL
	SELECT
		x.pipeline_code,
		x.good_sn,
		x.goods_spu,
		x.lang,
		x.category_id,
		x.lable_name,    
		0 as seq
	FROM
		dw_gearbest_recommend.gb_new_product_backup_parent_tmp x
;

--取2级分类 来补分类
INSERT OVERWRITE TABLE dw_gearbest_recommend.gb_new_product_backup_parent_tmp  SELECT
	m.pipeline_code,
	n.good_sn,
	n.goods_spu,
	m.lang,
	m.category_id,
    m.lable_name,    
    0 as seq
FROM
	(
		SELECT
			a.pipeline_code,
			a.category_id,
			a.lang,
            a.lable_name,
			b.level_2
		FROM
			(
				SELECT
					pipeline_code,
					category_id,
					lang,
                	lable_name,
					COUNT(goods_spu) cnt
				FROM
					dw_gearbest_recommend.gb_new_product_backup_parent_tmp
				GROUP BY
					pipeline_code,
					category_id,
					lang,
                	lable_name 
				HAVING
					cnt < 500
			) a
		JOIN ods.ods_o_gearbest_bigdata_goods_category_level b ON a.category_id = b.id
		WHERE b.day = '${DATE}'
	) m
JOIN dw_gearbest_recommend.gb_new_product_backup_parent_tmp n ON m.pipeline_code = n.pipeline_code
AND m.level_2 = n.category_id
AND m.lang = n.lang
AND m.lable_name = n.lable_name
UNION ALL
	SELECT
		x.pipeline_code,
		x.good_sn,
		x.goods_spu,
		x.lang,
		x.category_id,
		x.lable_name,    
		0 as seq
	FROM
		dw_gearbest_recommend.gb_new_product_backup_parent_tmp x
;

--取1级分类 来补分类
INSERT OVERWRITE TABLE dw_gearbest_recommend.gb_new_product_backup_parent_tmp  SELECT
	m.pipeline_code,
	n.good_sn,
	n.goods_spu,
	m.lang,
	m.category_id,
    m.lable_name,    
    0 as seq
FROM
	(
		SELECT
			a.pipeline_code,
			a.category_id,
			a.lang,
            a.lable_name,
			b.level_1
		FROM
			(
				SELECT
					pipeline_code,
					category_id,
					lang,
                	lable_name,
					COUNT(goods_spu) cnt
				FROM
					dw_gearbest_recommend.gb_new_product_backup_parent_tmp
				GROUP BY
					pipeline_code,
					category_id,
					lang,
                	lable_name 
				HAVING
					cnt < 500
			) a
		JOIN ods.ods_o_gearbest_bigdata_goods_category_level b ON a.category_id = b.id
		WHERE b.day = '${DATE}'
	) m
JOIN dw_gearbest_recommend.gb_new_product_backup_parent_tmp n ON m.pipeline_code = n.pipeline_code
AND m.level_1 = n.category_id
AND m.lang = n.lang
AND m.lable_name = n.lable_name
UNION ALL
	SELECT
		x.pipeline_code,
		x.good_sn,
		x.goods_spu,
		x.lang,
		x.category_id,
		x.lable_name,    
		0 as seq
	FROM
		dw_gearbest_recommend.gb_new_product_backup_parent_tmp x
;

--随机打散
INSERT overwrite TABLE dw_gearbest_recommend.gb_new_product_backup_parent_tmp  SELECT
	pipeline_code,
	good_sn,
	goods_spu,
	lang,
	category_id,
    lable_name,
    ROW_NUMBER () OVER (
        ORDER BY
            rand()
        ) AS seq
FROM dw_gearbest_recommend.gb_new_product_backup_current_tmp;