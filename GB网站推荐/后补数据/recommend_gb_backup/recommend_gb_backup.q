--@author ZhanRui
--@date 2018年10月17日 
--@desc  gb推荐后台后补数据，按pipeline_lang_categoryid分组，每组至多50个后补商品

SET mapred.job.name=goods_backup_result;
SET mapred.max.split.size=128000000;
SET mapred.min.split.size=32000000;
SET mapred.min.split.size.per.node=32000000;
SET mapred.min.split.size.per.rack=32000000;
SET hive.exec.reducers.bytes.per.reducer = 128000000;
SET hive.merge.mapfiles=true;
SET hive.merge.mapredfiles= true;
SET hive.merge.size.per.task=256000000; 

--pipeline_language_map更新
INSERT OVERWRITE TABLE dw_gearbest_recommend.pipeline_language_map  SELECT
	pipeline_code,
	CASE
WHEN lang = 'en-gb' THEN
	'en'
WHEN lang = 'en-us' THEN
	'en'
ELSE
	lang
END AS lang
FROM
	dw_gearbest_recommend.pipeline_language
WHERE
	STATUS = '1'
GROUP BY
	pipeline_code,
	lang
	;

 
 --商品池表更新，取pipeline_code对应的lang数据，其他多语言丢弃
 INSERT overwrite TABLE dw_gearbest_recommend.goods_info_result_uniqlang SELECT
	n.good_sn,
	n.goods_spu,
	n.goods_web_sku,
	n.shop_code,
	n.goods_status,
	n.brand_code,
	n.first_up_time,
	n.v_wh_code,
	n.shop_price,
	n.id,
	n.level_cnt,
	n.level_1,
	n.level_2,
	n.level_3,
	n.level_4,
	n.good_title,
	n.img_url,
	n.grid_url,
	n.thumb_url,
	n.thumb_extend_url,
	n.lang,
	n.stock_qty,
	n.avg_score,
	n.total_num,
	n.total_favorite,
	n.pipeline_code,
	n.url_title
FROM
	dw_gearbest_recommend.goods_info_result_uniq n
JOIN dw_gearbest_recommend.pipeline_language_map x ON n.pipeline_code = x.pipeline_code
AND n.lang = x.lang
;
 

--过滤网采商品/禁售商品
INSERT overwrite TABLE dw_gearbest_recommend.goods_info_result_uniqlang_filtered  SELECT
	n.*
FROM
	dw_gearbest_recommend.goods_info_result_uniqlang n
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
				UNION ALL
				select b.goods_sn as good_sn from  dw_gearbest_recommend.sku_not_sale b				
			) x
	)
;


 --后补基础数据准备，先随机取商品池每个pipeline_code、lang、id下的50个商品--200
 INSERT overwrite TABLE dw_gearbest_recommend.goods_info_result_backup  SELECT
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
			dw_gearbest_recommend.goods_info_result_uniqlang_filtered
		WHERE
			pipeline_code IS NOT NULL
		AND lang IS NOT NULL
		AND id IS NOT NULL
		--去除推荐位一二三四的sku数据--20190409--zhangyuchao
		AND good_sn IN (
				select a.good_sn
				from
				(
					select good_sn from dw_gearbest_recommend.goods_info_result_uniqlang_filtered 
					where pipeline_code IS NOT NULL 
					AND lang IS NOT NULL
					AND id IS NOT NULL
				) a
				left join
					(select DISTINCT goods_sn2 
					from dw_gearbest_recommend.gb_result_detail_page_gtq 
					where concat(year, month, day)=${ADD_TIME}) b
				on 
				   a.good_sn = b.goods_sn2 
				left join 
					(select DISTINCT goods_sn2 
					from dw_gearbest_recommend.gb_result_detail_1_page_gtq 
					where concat(year, month, day)=${ADD_TIME}) c
				on 
					a.good_sn = c.goods_sn2 
				left join
					( select distinct good_sn
								from dw_gearbest_recommend.apl_result_detail_page_sponsored_fact) d
				on a.good_sn = d.good_sn
				left join 
					(select distinct good_sn 
					from dw_gearbest_recommend.apl_lable_new_fact) e
				on a.good_sn = e.good_sn
				left join 
					(select distinct good_sn 
					from dw_gearbest_recommend.apl_lable_money_fact) f
				on a.good_sn = f.good_sn
				where b.goods_sn2 is null
				and c.goods_sn2 is null	
				and d.good_sn is null
				and e.good_sn is null
				and f.good_sn is null
		)
	) m
WHERE
	m.flag <= 200;


--各分类数据统计，商品池的商品是按照底级分类存的，向上构造父分类商品池
INSERT overwrite TABLE dw_gearbest_recommend.goods_backup_tmp  SELECT
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
			dw_gearbest_recommend.goods_info_result_backup a
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
				dw_gearbest_recommend.goods_info_result_backup a
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
					dw_gearbest_recommend.goods_info_result_backup a
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
						dw_gearbest_recommend.goods_info_result_backup a
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
	m.category_id;

--缩小数据量，每个分类取50个数据--200
INSERT overwrite TABLE dw_gearbest_recommend.goods_backup_tmp1  SELECT
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
			dw_gearbest_recommend.goods_backup_tmp
	) m
WHERE
	m.flag <= 200
;

--取3级分类 来补分类不够50个的情况--200
INSERT OVERWRITE TABLE dw_gearbest_recommend.goods_backup_autocomplete  SELECT
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
					dw_gearbest_recommend.goods_backup_tmp1
				GROUP BY
					pipeline_code,
					category_id,
					lang
				HAVING
					cnt < 200
			) a
		JOIN dw_gearbest_recommend.goods_category_level b ON a.category_id = b.id
	) m
JOIN dw_gearbest_recommend.goods_backup_tmp1 n ON m.pipeline_code = n.pipeline_code
AND m.level_3 = n.category_id
AND m.lang = n.lang
UNION ALL
	SELECT
		x.pipeline_code,
		x.goods_spu,
		x.lang,
		x.category_id
	FROM
		dw_gearbest_recommend.goods_backup_tmp1 x
;

--取2级分类 来补分类不够50个的情况--200
INSERT OVERWRITE TABLE dw_gearbest_recommend.goods_backup_autocomplete  SELECT
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
					dw_gearbest_recommend.goods_backup_autocomplete
				GROUP BY
					pipeline_code,
					category_id,
					lang
				HAVING
					cnt < 200
			) a
		JOIN dw_gearbest_recommend.goods_category_level b ON a.category_id = b.id
	) m
JOIN dw_gearbest_recommend.goods_backup_autocomplete n ON m.pipeline_code = n.pipeline_code
AND m.level_2 = n.category_id
AND m.lang = n.lang
UNION ALL
	SELECT
		x.pipeline_code,
		x.goods_spu,
		x.lang,
		x.category_id
	FROM
		dw_gearbest_recommend.goods_backup_autocomplete x
;

--取1级分类 来补分类不够50个的情况--200
INSERT OVERWRITE TABLE dw_gearbest_recommend.goods_backup_autocomplete  SELECT
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
					dw_gearbest_recommend.goods_backup_autocomplete
				GROUP BY
					pipeline_code,
					category_id,
					lang
				HAVING
					cnt < 200
			) a
		JOIN dw_gearbest_recommend.goods_category_level b ON a.category_id = b.id
	) m
JOIN dw_gearbest_recommend.goods_backup_autocomplete n ON m.pipeline_code = n.pipeline_code
AND m.level_1 = n.category_id
AND m.lang = n.lang
UNION ALL
	SELECT
		x.pipeline_code,
		x.goods_spu,
		x.lang,
		x.category_id
	FROM
		dw_gearbest_recommend.goods_backup_autocomplete x
;


--取15天热销商品，关联语言
INSERT overwrite TABLE dw_gearbest_recommend.goods_spu_hotsell_backup SELECT
	m.pipeline_code,
	m.goods_spu,
	n.lang,
	m.category_id
FROM
	dw_gearbest_recommend.goods_spu_hotsell_15days m
JOIN dw_gearbest_recommend.goods_info_result_uniq n ON m.pipeline_code = n.pipeline_code
AND m.goods_spu = n.goods_spu
--去除推荐位一二三四的sku--20190409--zhangyuchao
WHERE n.good_sn IN (
			select a.good_sn
			from
			(
				select good_sn from dw_gearbest_recommend.goods_info_result_uniq 
			) a
			left join
				(select DISTINCT goods_sn2 
				from dw_gearbest_recommend.gb_result_detail_page_gtq 
				where concat(year, month, day)=${ADD_TIME}) b
			on 
				a.good_sn = b.goods_sn2 
			left join 
				(select DISTINCT goods_sn2 
				from dw_gearbest_recommend.gb_result_detail_1_page_gtq 
				where concat(year, month, day)=${ADD_TIME}) c
			on 
				a.good_sn = c.goods_sn2 
			left join
				( select distinct good_sn
							from dw_gearbest_recommend.apl_result_detail_page_sponsored_fact) d
			on a.good_sn = d.good_sn
			left join 
				(select distinct good_sn 
				from dw_gearbest_recommend.apl_lable_new_fact) e
			on a.good_sn = e.good_sn
			left join 
				(select distinct good_sn 
				from dw_gearbest_recommend.apl_lable_money_fact) f
			on a.good_sn = f.good_sn
			where b.goods_sn2 is null
			and c.goods_sn2 is null	
			and d.good_sn is null
			and e.good_sn is null
			and f.good_sn is null
		)	
GROUP BY
	m.pipeline_code,
	m.goods_spu,
	n.lang,
	m.category_id;

--将15天热销商品和商品池后补数据汇总，缩小数据量，每个分类下取50个，去重--200
INSERT overwrite TABLE dw_gearbest_recommend.goods_backup_merge SELECT
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
					dw_gearbest_recommend.goods_spu_hotsell_backup
				UNION ALL
					SELECT
						pipeline_code,
						goods_spu,
						lang,
						category_id,
						1 AS rank
					FROM
						dw_gearbest_recommend.goods_backup_autocomplete
			) m
	) n
WHERE
	n.flag <= 200
group by 
	n.pipeline_code,
	n.goods_spu,
	n.lang,
	n.category_id
	;
		
--关联	pipeline_code 和 lang，确保剔除了pipeline_code下多语言的情况，缩小数据量
INSERT overwrite TABLE dw_gearbest_recommend.goods_backup_mid  SELECT
	n.pipeline_code,
	n.goods_spu,
	n.lang,
	n.category_id
FROM
	dw_gearbest_recommend.goods_backup_merge n
JOIN dw_gearbest_recommend.pipeline_language_map x ON n.pipeline_code = x.pipeline_code
AND n.lang = x.lang
;

--最终结果汇总,准备写入Redis的数据结果
INSERT overwrite TABLE dw_gearbest_recommend.goods_backup_result SELECT
	t1.good_sn,
	t3.goods_web_sku,
	t1.pipeline_code,
	t3.good_title,
	t1.lang,
	t1.category_id AS categoryid,
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
			dw_gearbest_recommend.goods_backup_mid m
		JOIN dw_gearbest_recommend.goods_info_mid5 n ON m.goods_spu = n.goods_spu
	) t1
JOIN 
( 
	SELECT n.* FROM
	dw_gearbest_recommend.goods_info_result_uniqlang n 
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
) t3
ON t1.good_sn = t3.good_sn
AND t1.pipeline_code = t3.pipeline_code
AND t1.lang = t3.lang
;
--20190409  zhangyuchao
--下面将结果表分为四个表，后补兜底推荐表，分别写入Redis，每个表进行插入至少35条
--保证pipeline_code,lang,categoryid维度下面，每一个组合都有至少35条
CREATE TABLE IF NOT EXISTS dw_gearbest_recommend.goods_backup_result_addflag_tmp(
  `good_sn` string, 
  `webgoodsn` string, 
  `pipeline_code` string, 
  `goodstitle` string, 
  `lang` string, 
  `categoryid` int, 
  `warecode` int, 
  `reviewcount` bigint, 
  `avgrate` double, 
  `shopprice` double, 
  `favoritecount` int, 
  `goodsnum` int, 
  `imgurl` string, 
  `gridurl` string, 
  `thumburl` string, 
  `thumbextendurl` string, 
  `url_title` string,
  `flag` int
  )
COMMENT '推荐位后补兜底数据增加排序序列'
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\u0001'                                                                                   
LINES TERMINATED BY '\n'                                                                                          
STORED AS TEXTFILE;

CREATE TABLE IF NOT EXISTS dw_gearbest_recommend.goods_backup_result_one(
  `good_sn` string, 
  `webgoodsn` string, 
  `pipeline_code` string, 
  `goodstitle` string, 
  `lang` string, 
  `categoryid` int, 
  `warecode` int, 
  `reviewcount` bigint, 
  `avgrate` double, 
  `shopprice` double, 
  `favoritecount` int, 
  `goodsnum` int, 
  `imgurl` string, 
  `gridurl` string, 
  `thumburl` string, 
  `thumbextendurl` string, 
  `url_title` string
  )
COMMENT '第一推荐位后补兜底数据'
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\u0001'                                                                                   
LINES TERMINATED BY '\n'                                                                                          
STORED AS TEXTFILE;

CREATE TABLE IF NOT EXISTS dw_gearbest_recommend.goods_backup_result_two(
  `good_sn` string, 
  `webgoodsn` string, 
  `pipeline_code` string, 
  `goodstitle` string, 
  `lang` string, 
  `categoryid` int, 
  `warecode` int, 
  `reviewcount` bigint, 
  `avgrate` double, 
  `shopprice` double, 
  `favoritecount` int, 
  `goodsnum` int, 
  `imgurl` string, 
  `gridurl` string, 
  `thumburl` string, 
  `thumbextendurl` string, 
  `url_title` string
  )
COMMENT '第二推荐位后补兜底数据'
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\u0001'                                                                                   
LINES TERMINATED BY '\n'                                                                                          
STORED AS TEXTFILE;

CREATE TABLE IF NOT EXISTS dw_gearbest_recommend.goods_backup_result_three(
  `good_sn` string, 
  `webgoodsn` string, 
  `pipeline_code` string, 
  `goodstitle` string, 
  `lang` string, 
  `categoryid` int, 
  `warecode` int, 
  `reviewcount` bigint, 
  `avgrate` double, 
  `shopprice` double, 
  `favoritecount` int, 
  `goodsnum` int, 
  `imgurl` string, 
  `gridurl` string, 
  `thumburl` string, 
  `thumbextendurl` string, 
  `url_title` string
  )
COMMENT '第三推荐位后补兜底数据'
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\u0001'                                                                                   
LINES TERMINATED BY '\n'                                                                                          
STORED AS TEXTFILE;

CREATE TABLE IF NOT EXISTS dw_gearbest_recommend.goods_backup_result_four(
  `good_sn` string, 
  `webgoodsn` string, 
  `pipeline_code` string, 
  `goodstitle` string, 
  `lang` string, 
  `categoryid` int, 
  `warecode` int, 
  `reviewcount` bigint, 
  `avgrate` double, 
  `shopprice` double, 
  `favoritecount` int, 
  `goodsnum` int, 
  `imgurl` string, 
  `gridurl` string, 
  `thumburl` string, 
  `thumbextendurl` string, 
  `url_title` string
  )
COMMENT '第四推荐位后补兜底数据'
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\u0001'                                                                                   
LINES TERMINATED BY '\n'                                                                                          
STORED AS TEXTFILE;

--向goods_backup_result_addflag_tmp 中导入goods_backup_result数据进行排序存储--增加一个flag
INSERT overwrite TABLE dw_gearbest_recommend.goods_backup_result_addflag_tmp  SELECT
		  t.good_sn, 
		  t.webgoodsn, 
		  t.pipeline_code, 
		  t.goodstitle, 
		  t.lang, 
		  t.categoryid, 
		  t.warecode, 
		  t.reviewcount, 
		  t.avgrate, 
		  t.shopprice, 
		  t.favoritecount, 
		  t.goodsnum, 
		  t.imgurl, 
		  t.gridurl, 
		  t.thumburl, 
		  t.thumbextendurl, 
		  t.url_title,
		  t.flag
FROM
(
SELECT
		  good_sn, 
		  webgoodsn, 
		  pipeline_code, 
		  goodstitle, 
		  lang, 
		  categoryid, 
		  warecode, 
		  reviewcount, 
		  avgrate, 
		  shopprice, 
		  favoritecount, 
		  goodsnum, 
		  imgurl, 
		  gridurl, 
		  thumburl, 
		  thumbextendurl, 
		  url_title,
		  ROW_NUMBER () OVER (
				PARTITION BY pipeline_code, 
				lang,
				categoryid
			ORDER BY
				rand()
			) AS flag		
FROM 
		dw_gearbest_recommend.goods_backup_result 
WHERE
			pipeline_code IS NOT NULL
		AND lang IS NOT NULL
		AND categoryid IS NOT NULL
) t ;

--推荐位1,准备写入Redis的数据结果
INSERT overwrite TABLE dw_gearbest_recommend.goods_backup_result_one SELECT
  t.good_sn, 
  t.webgoodsn, 
  t.pipeline_code, 
  t.goodstitle, 
  t.lang, 
  t.categoryid, 
  t.warecode, 
  t.reviewcount, 
  t.avgrate, 
  t.shopprice, 
  t.favoritecount, 
  t.goodsnum, 
  t.imgurl, 
  t.gridurl, 
  t.thumburl, 
  t.thumbextendurl, 
  t.url_title
FROM
	dw_gearbest_recommend.goods_backup_result_addflag_tmp t 
WHERE 
	t.flag <=35 ;

--推荐位2,准备写入Redis的数据结果
INSERT overwrite TABLE dw_gearbest_recommend.goods_backup_result_two SELECT
  t.good_sn, 
  t.webgoodsn, 
  t.pipeline_code, 
  t.goodstitle, 
  t.lang, 
  t.categoryid, 
  t.warecode, 
  t.reviewcount, 
  t.avgrate, 
  t.shopprice, 
  t.favoritecount, 
  t.goodsnum, 
  t.imgurl, 
  t.gridurl, 
  t.thumburl, 
  t.thumbextendurl, 
  t.url_title
FROM
	dw_gearbest_recommend.goods_backup_result_addflag_tmp t 
WHERE 
	t.flag > 35 and t.flag <=70 ;


--推荐位3,准备写入Redis的数据结果
INSERT overwrite TABLE dw_gearbest_recommend.goods_backup_result_three SELECT
  t.good_sn, 
  t.webgoodsn, 
  t.pipeline_code, 
  t.goodstitle, 
  t.lang, 
  t.categoryid, 
  t.warecode, 
  t.reviewcount, 
  t.avgrate, 
  t.shopprice, 
  t.favoritecount, 
  t.goodsnum, 
  t.imgurl, 
  t.gridurl, 
  t.thumburl, 
  t.thumbextendurl, 
  t.url_title
FROM
	dw_gearbest_recommend.goods_backup_result_addflag_tmp t 
WHERE 
	t.flag > 70 and t.flag <=105 ;

--推荐位4,准备写入Redis的数据结果
INSERT overwrite TABLE dw_gearbest_recommend.goods_backup_result_four SELECT
  t.good_sn, 
  t.webgoodsn, 
  t.pipeline_code, 
  t.goodstitle, 
  t.lang, 
  t.categoryid, 
  t.warecode, 
  t.reviewcount, 
  t.avgrate, 
  t.shopprice, 
  t.favoritecount, 
  t.goodsnum, 
  t.imgurl, 
  t.gridurl, 
  t.thumburl, 
  t.thumbextendurl, 
  t.url_title
FROM
	dw_gearbest_recommend.goods_backup_result_addflag_tmp t 
WHERE 
	t.flag > 105 ;






   
     
                  