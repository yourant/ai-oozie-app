--@author LiuQingFan
--@date 2017年11月25日 下午 16:00
--@desc  首页推荐结果

set mapred.job.queue.name=root.bigdata.offline; 
set mapred.job.name='apl_result_homepage_recommend_gb_fact';
USE  dw_gearbest_recommend;
set mapred.max.split.size=100000000;
set mapred.min.split.size.per.node=100000000;
set mapred.min.split.size.per.rack=100000000;
set hive.auto.convert.join = false; 
set hive.exec.reducers.bytes.per.reducer=500000000;
set hive.groupby.skewindata=true;
set hive.input.format=org.apache.hadoop.hive.ql.io.BucketizedHiveInputFormat;
----无cookie用户
CREATE TABLE IF NOT EXISTS apl_homepage_unexists_cookie_fact(
	nodes                string        COMMENT '分类',
	goods_sn             string        COMMENT '商品SKU',
	goods_id             bigint        COMMENT '商品自增ID',
	wid                  int           COMMENT '仓库ID',
	shop_price           double        COMMENT '本店售价',
	goods_title          string    	   COMMENT '商品标题（网站前端显示）',
	goods_thumb          string    	   COMMENT '商品缩略图(100*100)',
	goods_grid           string    	   COMMENT '商品缩略图（150*150）',
	goods_img            string    	   COMMENT '商品缩略图（250*250）',
	original_img         string    	   COMMENT '商品原图',
	url_title            String        COMMENT 'url使用标题'
	)
COMMENT '首页无COOKIE推荐结果'
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\u0001'                                                                                   
LINES TERMINATED BY '\n'                                                                                          
STORED AS TEXTFILE;


INSERT OVERWRITE TABLE apl_homepage_unexists_cookie_mid
SELECT
	goods_spu,
	node1
FROM(
	SELECT
		node1,
		goods_spu,
		ROW_NUMBER() OVER(PARTITION BY node1 ORDER BY rand() ) AS flag
	FROM(
		SELECT
			a.goods_spu,
			b.node1
		FROM(
			SELECT
				a.goods_spu,
				b.node1
			FROM
				(SELECT
					goods_spu
				FROM
					goods_spu_hotsell_30days
				WHERE
					pipeline_code = 'GB'
				) a
			JOIN
				goods_info_mid5 b
			ON
				a.goods_spu = b.goods_spu
			) a
		JOIN
			(
			SELECT
				node1,
				sellqty
			FROM(
				SELECT
					node1,
					SUM(sellcount) sellqty
				FROM(
					SELECT
						a.goods_spu,
						a.sellcount,
						b.node1
					FROM
						(SELECT
							goods_spu,
							category_id,
							qty sellcount
						FROM
							goods_spu_order_count_30days
						WHERE
							pipeline_code = 'GB'
						)a
					JOIN
						goods_info_mid5 b
					ON
						a.goods_spu = b.goods_spu
					) a
				GROUP BY
					node1
				)a
			ORDER BY
					sellqty DESC LIMIT 10
			) b
		ON
			a.node1 = b.node1
		)tmp
	)tmp
WHERE
	flag < 3 ;
	
	
	

----有COOKIE用户
INSERT OVERWRITE TABLE apl_homepage_recommend_mid 
SELECT
	goods_spu1,
	goods_spu2
FROM(
		SELECT
		goods_spu1,
		goods_spu2,
		ROW_NUMBER() OVER(PARTITION BY goods_spu1 ORDER BY sellcount desc) AS flag
	FROM(
		SELECT
			t1.goods_spu goods_spu1,
			t2.goods_spu goods_spu2,
			t2.sellcount,
			t1.level_4
		FROM
			(SELECT
				goods_spu,
				level_4
			FROM
				goods_info_result
			GROUP BY
				goods_spu,
				level_4
			)t1
		JOIN
			(SELECT
				goods_spu,
				pipeline_code,
				category_id,
				sellcount
			FROM
				goods_spu_hotsell_30days
			WHERE
				pipeline_code = 'GB'
			)t2
		ON
			t1.level_4 = t2.category_id 
		
		) tmp
	)tmp
WHERE
	flag < 16 AND goods_spu1 != goods_spu2;
	
	

	
INSERT INTO  apl_homepage_recommend_mid 
SELECT
	goods_spu1,
	goods_spu2
FROM(
		SELECT
		goods_spu1,
		goods_spu2,
		ROW_NUMBER() OVER(PARTITION BY goods_spu1 ORDER BY sellcount desc) AS flag
	FROM(
		SELECT
			t1.goods_spu goods_spu1,
			t2.goods_spu goods_spu2,
			t2.sellcount,
			t1.level_3
		FROM
			(SELECT
				goods_spu,
				level_3
			FROM
				goods_info_result a
			WHERE
					a.goods_spu NOT IN
						(SELECT
							d.goods_spu1
						FROM
							(SELECT
								b.goods_spu1,COUNT(DISTINCT goods_spu2) c
							FROM
								apl_homepage_recommend_mid b
							GROUP BY 
								b.goods_spu1
							HAVING
								c > 14
							)d 
						)
			GROUP BY
				goods_spu,
				level_3
			)t1
		JOIN
			(SELECT
				goods_spu,
				pipeline_code,
				category_id,
				sellcount
			FROM
				goods_spu_hotsell_30days
			WHERE
				pipeline_code = 'GB'
			) t2
		WHERE
			t1.level_3 = t2.category_id
		) tmp
	)tmp
WHERE
	flag < 16 AND goods_spu1 != goods_spu2;
	
INSERT INTO  apl_homepage_recommend_mid 
SELECT
	goods_spu1,
	goods_spu2
FROM(
		SELECT
		goods_spu1,
		goods_spu2,
		ROW_NUMBER() OVER(PARTITION BY goods_spu1 ORDER BY sellcount desc) AS flag
	FROM(
		SELECT
			t1.goods_spu goods_spu1,
			t2.goods_spu goods_spu2,
			t2.sellcount,
			t1.level_2
		FROM
			(SELECT
				goods_spu,
				level_2
			FROM
				goods_info_mid1 a
			WHERE
					a.goods_spu NOT IN
						(SELECT
							d.goods_spu1
						FROM
							(SELECT
								b.goods_spu1,COUNT(DISTINCT goods_spu2) c
							FROM
								apl_homepage_recommend_mid b
							GROUP BY 
								b.goods_spu1
							HAVING
								c > 14
							)d 
						)
			GROUP BY
				goods_spu,
				level_2
			)t1
		JOIN
			(SELECT
				goods_spu,
				pipeline_code,
				category_id,
				sellcount
			FROM
				goods_spu_hotsell_30days
			WHERE
				pipeline_code = 'GB'
			) t2
		WHERE
			t1.level_2 = t2.category_id
		) tmp
	)tmp
WHERE
	flag < 16 AND goods_spu1 != goods_spu2 ;


INSERT INTO  apl_homepage_recommend_mid 
SELECT
	goods_spu1,
	goods_spu2
FROM(
		SELECT
		goods_spu1,
		goods_spu2,
		ROW_NUMBER() OVER(PARTITION BY goods_spu1 ORDER BY sellcount desc) AS flag
	FROM(
		SELECT
			t1.goods_spu goods_spu1,
			t2.goods_spu goods_spu2,
			t2.sellcount,
			t1.level_1
		FROM
			(SELECT
				goods_spu,
				level_1
			FROM
				goods_info_mid1 a
			WHERE
					a.goods_spu NOT IN
						(SELECT
							d.goods_spu1
						FROM
							(SELECT
								b.goods_spu1,COUNT(DISTINCT goods_spu2) c
							FROM
								apl_homepage_recommend_mid b
							GROUP BY 
								b.goods_spu1
							HAVING
								c > 14
							)d 
						)
			GROUP BY
				goods_spu,
				level_1
			)t1
		JOIN
			(SELECT
				goods_spu,
				pipeline_code,
				category_id,
				sellcount
			FROM
				goods_spu_hotsell_30days
			WHERE
				pipeline_code = 'GB'
			) t2
		ON
			t1.level_1 = t2.category_id
		) tmp
	)tmp
WHERE
	flag < 16 AND goods_spu1 != goods_spu2;
	
	
	

	
CREATE TABLE IF NOT EXISTS apl_result_rtiyv_gb_uncookie_fact(
	good_sn            string     COMMENT '商品SKU',
	webGoodSn          string     COMMENT '商品webSku',
	goodsTitle         string     COMMENT '商品title',
	lang               string     COMMENT '语言',
	wareCode           int        COMMENT '仓库',
	reviewCount        int        COMMENT '评论数',
	avgRate            double     COMMENT '评论分', 
	shopPrice          double     COMMENT '本店售价',
	favoriteCount      int 	      COMMENT '收藏数量',
	goodsNum           int        COMMENT '商品库存',
	imgUrl             string     COMMENT '产品图url',
	gridUrl            string     COMMENT 'grid图url',
	thumbUrl           string     COMMENT '缩略图url',
	thumbExtendUrl     string     COMMENT '商品图片',
	url_title          string     COMMENT '静态页面文件标题'
	)
COMMENT '首页推荐结果（无COOKIE）'
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\u0001'                                                                                   
LINES TERMINATED BY '\n'                                                                                          
STORED AS TEXTFILE;


INSERT OVERWRITE TABLE apl_result_rtiyv_gb_uncookie_fact
SELECT
	good_sn,
	goods_web_sku,
	good_title,
	lang,
	v_wh_code,
	total_num,
	avg_score,
	shop_price,
	total_favorite,
	stock_qty,
	img_url,
	grid_url,
	thumb_url,
	thumb_extend_url,
	url_title
FROM(
	SELECT
	good_sn,
	goods_web_sku,
	good_title,
	lang,
	v_wh_code,
	total_num,
	avg_score,
	shop_price,
	total_favorite,
	stock_qty,
	img_url,
	grid_url,
	thumb_url,
	thumb_extend_url,
	url_title,
	ROW_NUMBER() OVER(PARTITION BY good_sn,good_title,lang ORDER BY shop_price ASC) AS flag
FROM(
	SELECT
		good_sn,
		goods_web_sku,
		good_title,
		lang,
		v_wh_code,
		total_num,
		avg_score,
		shop_price,
		total_favorite,
		stock_qty,
		img_url,
		grid_url,
		thumb_url,
		thumb_extend_url,
		url_title
	FROM(
		SELECT
				t2.good_sn,
				t3.goods_web_sku,
				t3.good_title,
				t3.lang,
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
				apl_homepage_unexists_cookie_mid t1
			JOIN
				goods_info_mid5 t2
			ON
				t1.goods_spu = t2.goods_spu
			JOIN(
				SELECT
					good_sn,
					goods_web_sku,
					good_title,
					lang,
					v_wh_code,
					total_num,
					avg_score,
					shop_price,
					total_favorite,
					stock_qty,
					img_url,
					grid_url,
					thumb_url,
					thumb_extend_url,
					url_title
				FROM
					goods_info_result_rec a
				WHERE
					a.id NOT IN 
							(SELECT 
								id 
							FROM 
								goods_category_level 
							WHERE
								r_path like concat('%','11546','%') 
								OR r_path like concat('%','11502','%')
								OR r_path like concat('%','11281','%')
								OR r_path like concat('%','12433','%')
								OR r_path like concat('%','11380','%')
								OR r_path like concat('%','12181','%')
								OR r_path like concat('%','12056','%')
	
						) AND pipeline_code = 'GB'
			
				) t3
			ON
				t2.good_sn = t3.good_sn
			)tmp
		GROUP BY
			good_sn,
			goods_web_sku,
			good_title,
			lang,
			v_wh_code,
			total_num,
			avg_score,
			shop_price,
			total_favorite,
			stock_qty,
			img_url,
			grid_url,
			thumb_url,
			thumb_extend_url,
			url_title
		)tmp
	)tmp
WHERE
	flag = 1
;
	

	
	

	
CREATE TABLE IF NOT EXISTS apl_result_rtiyv_gb_cookie_fact(
	good_sn1           string     COMMENT '主商品SKU',
	good_sn2           string     COMMENT '推荐商品SKU',
	webGoodSn          string     COMMENT '商品webSku',
	goodsTitle         string     COMMENT '商品title',
	lang               string     COMMENT '语言',
	wareCode           int        COMMENT '仓库',
	reviewCount        int        COMMENT '评论数',
	avgRate            double     COMMENT '评论分',
	shopPrice          double     COMMENT '本店售价',
	favoriteCount      int 	      COMMENT '收藏数量',
	goodsNum           int        COMMENT '商品库存',
	imgUrl             string     COMMENT '产品图url',
	gridUrl            string     COMMENT 'grid图url',
	thumbUrl           string     COMMENT '缩略图url',
	thumbExtendUrl     string     COMMENT '商品图片',
	url_title          string     COMMENT '静态页面文件标题'
	)
COMMENT '首页推荐结果（有COOKIE）'
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\u0001'                                                                                   
LINES TERMINATED BY '\n'                                                                                          
STORED AS TEXTFILE;



INSERT OVERWRITE TABLE apl_result_rtiyv_gb_cookie_fact
SELECT
	good_sn1,
	good_sn2,
	goods_web_sku,
	good_title,
	lang,
	v_wh_code,
	total_num,
	avg_score,
	shop_price,
	total_favorite,
	stock_qty,
	img_url,
	grid_url,
	thumb_url,
	thumb_extend_url,
	url_title
	FROM(
		SELECT
			good_sn1,
			good_sn2,
			goods_web_sku,
			good_title,
			lang,
			v_wh_code,
			total_num,
			avg_score,
			shop_price,
			total_favorite,
			stock_qty,
			img_url,
			grid_url,
			thumb_url,
			thumb_extend_url,
			url_title
		FROM(
			SELECT
				t2.good_sn good_sn1,
				t1.good_sn good_sn2,
				t1.goods_web_sku,
				t1.good_title,
				t1.lang,
				t1.v_wh_code,
				t1.total_num,
				t1.avg_score,
				t1.shop_price,
				t1.total_favorite,
				t1.stock_qty,
				t1.img_url,
				t1.grid_url,
				t1.thumb_url,
				t1.thumb_extend_url,
				t1.url_title
			FROM(
				SELECT
					t1.goods_spu1,
					t2.good_sn,
					t3.goods_web_sku,
					t3.good_title,
					t3.lang,
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
					apl_homepage_recommend_mid t1
				JOIN
					goods_info_mid5 t2
				ON
					t1.goods_spu2 = t2.goods_spu
				JOIN(
					SELECT
						good_sn,
						goods_web_sku,
						good_title,
						lang,
						v_wh_code,
						total_num,
						avg_score,
						shop_price,
						total_favorite,
						stock_qty,
						img_url,
						grid_url,
						thumb_url,
						thumb_extend_url,
						url_title
					FROM
						goods_info_result_uniqlang a
					WHERE
						a.id NOT IN 
								(SELECT 
									id 
								FROM 
									goods_category_level 
								WHERE
									r_path like concat('%','11546','%') 
									OR r_path like concat('%','11502','%')
									OR r_path like concat('%','11281','%')
									OR r_path like concat('%','12433','%')
									OR r_path like concat('%','11380','%')
									OR r_path like concat('%','12181','%')
									OR r_path like concat('%','12056','%')
	
							) AND pipeline_code = 'GB'
					)t3
				ON
					t2.good_sn = t3.good_sn
				) t1
			JOIN
				goods_info_result t2
			ON
				t1.goods_spu1 = t2.goods_spu
			)tmp
		GROUP BY
			good_sn1,
			good_sn2,
			goods_web_sku,
			good_title,
			lang,
			v_wh_code,
			total_num,
			avg_score,
			shop_price,
			total_favorite,
			stock_qty,
			img_url,
			grid_url,
			thumb_url,
			thumb_extend_url,
			url_title
		)tmp
WHERE
    good_sn1 != good_sn2
;
