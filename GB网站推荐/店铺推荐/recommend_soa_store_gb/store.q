
--@author LiuQingFan
--@date 2017年11月25日 下午 16:00
--@desc  商详页Recommended Products For You 店铺商品推荐过程

set mapred.job.queue.name=root.ai.online; 
set mapred.job.name='apl_result_rpfy_store_gb_fact';
USE  dw_gearbest_recommend;
set mapred.max.split.size=100000000;
set mapred.min.split.size.per.node=100000000;
set mapred.min.split.size.per.rack=100000000;
set hive.auto.convert.join = false; 
set hive.exec.reducers.bytes.per.reducer=500000000;
set hive.groupby.skewindata=true;
set hive.input.format=org.apache.hadoop.hive.ql.io.BucketizedHiveInputFormat;
----店铺商品
CREATE TABLE IF NOT EXISTS apl_store_recommend_mid(
	goods_spu1     string    COMMENT '主商品SKU',
	goods_spu2     string    COMMENT '推荐商品SKU',
	shop_code      string    COMMENT '店铺ID'
	)
COMMENT '店铺商品推荐结果'
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\u0001'                                                                                   
LINES TERMINATED BY '\n'                                                                                          
STORED AS TEXTFILE;


INSERT OVERWRITE TABLE apl_store_recommend_mid
SELECT
	goods_spu1,
	goods_spu2,
	shop_code
FROM(
	SELECT
		goods_spu1,
		goods_spu2,
		shop_code,
		ROW_NUMBER() OVER(PARTITION BY goods_spu1 ORDER BY score DESC) AS flag
	FROM(
		SELECT
			t1.goods_spu goods_spu1,
			t2.goods_spu goods_spu2,
			t1.shop_code,
			4  score
		FROM
			(SELECT
				goods_spu,
				id,
				shop_code
			FROM
				goods_info_result
			WHERE
				shop_code != 888888888888888888
			GROUP BY
				goods_spu,
				id,
				shop_code
			)t1
		JOIN
			(SELECT
				goods_spu,
				id,
				shop_code
			FROM
				goods_info_result
			WHERE
				shop_code != 888888888888888888
			GROUP BY
				goods_spu,
				id,
				shop_code
			)t2
		ON
			t1.shop_code = t2.shop_code AND t1.id = t2.id
		WHERE
			t1.goods_spu != t2.goods_spu
		UNION ALL
		SELECT
			t1.goods_spu goods_spu1,
			t2.goods_spu goods_spu2,
			t1.shop_code,
			3  score
		FROM
			(SELECT
				goods_spu,
				id,
				shop_code
			FROM
				goods_info_result
			WHERE
				shop_code != 888888888888888888
			GROUP BY
				goods_spu,
				id,
				shop_code
			)t1
		JOIN
			(SELECT
				goods_spu,
				id,
				shop_code
			FROM
				goods_info_result
			WHERE
				shop_code != 888888888888888888
			GROUP BY
				goods_spu,
				id,
				shop_code
			)t2
		ON
			t1.shop_code = t2.shop_code
		WHERE
			t1.id != t2.id AND t1.goods_spu != t2.goods_spu 
		)tmp
	)tmp
WHERE
	flag < 16;
	
	
CREATE TABLE IF NOT EXISTS apl_result_rpfy_store_gb_fact(
	good_sn1           string     COMMENT '主商品SKU',
	good_sn2           string     COMMENT '推荐商品SKU',
	pipeline_code      string     COMMENT '渠道编码',
	shop_code          string     COMMENT '店铺ID',
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
COMMENT '店铺商品推荐结果'
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\u0001'                                                                                   
LINES TERMINATED BY '\n'                                                                                          
STORED AS TEXTFILE;

INSERT OVERWRITE TABLE apl_result_rpfy_store_gb_fact
SELECT
	good_sn1,
	good_sn2,
	pipeline_code,
	shop_code,
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
		shop_code,
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
		ROW_NUMBER() OVER(PARTITION BY good_sn1,good_sn2,pipeline_code,shop_code,good_title,lang ORDER BY shop_price ASC) AS flag
	FROM(
		SELECT
			good_sn1,
			good_sn2,
			pipeline_code,
			shop_code,
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
				t1.shop_code,
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
					t3.pipeline_code,
					t1.shop_code,
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
					apl_store_recommend_mid t1
				JOIN
					goods_info_mid5 t2
				ON
					t1.goods_spu2 = t2.goods_spu
				JOIN
					goods_info_result_rec t3
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
			pipeline_code,
			shop_code,
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
