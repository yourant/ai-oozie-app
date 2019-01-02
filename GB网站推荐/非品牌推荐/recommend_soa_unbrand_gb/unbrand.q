--@author LiuQingFan
--@date 2018年03月28日 下午 16:00
--@desc  商详页Recommended Products For You 非品牌商品推荐过程

set mapred.job.queue.name=root.ai.online; 
set mapred.job.name='apl_result_rpfy_unbrand_gb_fact';
USE  dw_gearbest_recommend;
set mapred.max.split.size=100000000;
set mapred.min.split.size.per.node=100000000;
set mapred.min.split.size.per.rack=100000000;
set hive.auto.convert.join = false; 
set hive.exec.reducers.bytes.per.reducer=500000000;
set hive.groupby.skewindata=true;
set hive.input.format=org.apache.hadoop.hive.ql.io.BucketizedHiveInputFormat;
CREATE TABLE IF NOT EXISTS apl_unbrand_recommend_mid(
	goods_spu1           string     COMMENT '主商品Spu',
	goods_spu2           string     COMMENT '推荐商品Spu',
	pipeline_code        string     COMMENT '渠道编码', 
	sellcount            string     COMMENT '销量',
	score                string     COMMENT '等级评分'
	)
COMMENT '非品牌推荐结果SPU'
CLUSTERED BY (pipeline_code) INTO 11 BUCKETS
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\u0001'                                                                                   
LINES TERMINATED BY '\n'                                                                                          
STORED AS TEXTFILE;	



INSERT OVERWRITE TABLE apl_unbrand_recommend_mid
SELECT
	goods_spu1,
	goods_spu2,
	pipeline_code,
	sellcount,
	score
FROM(
	SELECT
		goods_spu1,
		goods_spu2,
		pipeline_code,
		sellcount,
		score,
		ROW_NUMBER() OVER(PARTITION BY goods_spu1,pipeline_code ORDER BY sellcount DESC) AS flag
	FROM(
		SELECT
			t1.goods_spu goods_spu1,
			t2.goods_spu goods_spu2,
			t2.pipeline_code,
			t2.sellcount,
			4 score
		FROM
			(SELECT
				goods_spu,
				pipeline_code,
				level_4
			FROM 
				goods_info_result
			GROUP BY
				goods_spu,
				pipeline_code,
				level_4
				)t1
		JOIN
			goods_spu_hotsell_15days t2
		ON
			t1.level_4 = t2.category_id AND t1.pipeline_code = t2.pipeline_code
		)tmp
	)t
WHERE
	flag < 31
;
	
	
	
	
INSERT into TABLE apl_unbrand_recommend_mid
SELECT
	goods_spu1,
	goods_spu2,
	pipeline_code,
	sellcount,
	score
FROM(
	SELECT
		goods_spu1,
		goods_spu2,
		pipeline_code,
		sellcount,
		score,
		ROW_NUMBER() OVER(PARTITION BY goods_spu1,pipeline_code ORDER BY sellcount DESC) AS flag
	FROM(
		SELECT
			t1.goods_spu goods_spu1,
			t2.goods_spu goods_spu2,
			t2.pipeline_code,
			t2.sellcount,
			3 score
		FROM
			(SELECT
				a.goods_spu,
				a.pipeline_code,
				a.level_3
			FROM
				goods_info_result a
			WHERE
				a.goods_spu NOT IN
								(SELECT
									d.goods_spu1
								FROM
									(SELECT
										b.pipeline_code,b.goods_spu1,COUNT(*) c
									FROM
										apl_unbrand_recommend_mid b
									GROUP BY 
										b.goods_spu1,b.pipeline_code
									HAVING
										c > 29
									)d
								)
			GROUP BY
				a.goods_spu,
				a.pipeline_code,
				a.level_3
			)t1
		JOIN
			goods_spu_hotsell_15days t2
		ON
			t1.level_3 = t2.category_id AND t1.pipeline_code = t2.pipeline_code
		)tmp
	)t
WHERE
	flag < 31
;
		



CREATE TABLE IF NOT EXISTS apl_result_rpfy_unbrand_gb_fact(
	good_sn1           string     COMMENT '主商品SKU',
	good_sn2           string     COMMENT '推荐商品SKU',
	pipeline_code      string     COMMENT '渠道编码', 
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
COMMENT '非品牌推荐结果'
CLUSTERED BY (pipeline_code) INTO 11 BUCKETS
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\u0001'                                                                                   
LINES TERMINATED BY '\n'                                                                                          
STORED AS TEXTFILE;	
	
INSERT OVERWRITE TABLE apl_result_rpfy_unbrand_gb_fact
SELECT
	good_sn1,
	good_sn2,
	pipeline_code,
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
		pipeline_code,
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
		ROW_NUMBER() OVER(PARTITION BY good_sn1,good_sn2,pipeline_code,good_title,lang ORDER BY shop_price ASC) AS flag
	FROM(
		SELECT
			good_sn1,
			good_sn2,
			pipeline_code,
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
				t1.pipeline_code,
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
					t1.pipeline_code,
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
					apl_unbrand_recommend_mid t1
				JOIN
					goods_info_mid5 t2
				ON
					t1.goods_spu2 = t2.goods_spu
				JOIN
					goods_info_result_rec t3
				ON
					t2.good_sn = t3.good_sn AND t1.pipeline_code = t3.pipeline_code
				) t1
			JOIN
				goods_info_result t2
			ON
				t1.goods_spu1 = t2.goods_spu
			)tmp
		WHERE
			good_sn1 != good_sn2
		GROUP BY
			good_sn1,
			good_sn2,
			pipeline_code,
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
	flag = 1 AND good_sn1 != good_sn2
;