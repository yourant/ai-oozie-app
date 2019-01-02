--@author LiuQingFan
--@date 2018年03月28日 下午 16:00
--@desc  用户中心页推荐过程
set mapred.job.queue.name=root.ai.online; 
set mapred.job.name='apl_user_page_fact';
USE  dw_gearbest_recommend;

CREATE TABLE IF NOT EXISTS apl_female_ymal_fact(
	category_id        string     COMMENT '商品分类',
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
COMMENT '女性推荐商品'
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\u0001'                                                                                   
LINES TERMINATED BY '\n'                                                                                          
STORED AS TEXTFILE;



INSERT OVERWRITE TABLE apl_female_ymal_fact
SELECT
	category_id,
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
		category_id,
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
			category_id,
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
				t1.category_id,
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
			FROM(
				SELECT
					goods_spu,
					category_id
				FROM
					goods_spu_hotsell_30days
				WHERE
					pipeline_code = 'GB' AND category_id = 12084
				UNION ALL
				SELECT
					goods_spu,
					category_id
				FROM
					goods_hotsell_30days_supply
				WHERE
					pipeline_code = 'GB'
				) t1
			JOIN
				goods_info_mid5 t2
			ON
				t1.goods_spu = t2.goods_spu
			JOIN
				goods_info_result_rec t3
			ON
				t2.good_sn = t3.good_sn
			WHERE
				t3.pipeline_code = 'GB'
			)tmp
		GROUP BY
			category_id,
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



CREATE TABLE IF NOT EXISTS apl_cart_10_fact(
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
COMMENT '用户收藏top10'
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\u0001'                                                                                   
LINES TERMINATED BY '\n'                                                                                          
STORED AS TEXTFILE;


INSERT OVERWRITE TABLE apl_cart_10_fact
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
			FROM(
				SELECT
					goods_spu
				FROM
					gb_platform_favorite_top10
				WHERE
					pipeline_code = 'GB'
				) t1
			JOIN
				goods_info_mid5 t2
			ON
				t1.goods_spu = t2.goods_spu
			JOIN
				goods_info_result_rec t3
			ON
				t2.good_sn = t3.good_sn
			WHERE
				t3.pipeline_code = 'GB'
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