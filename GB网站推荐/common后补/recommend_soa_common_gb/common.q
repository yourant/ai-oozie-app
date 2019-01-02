--@author LiuQingFan
--@date 2018年03月28日 下午 16:00
--@desc  分类销量、网站热销统计
set mapred.job.name='apl_result_common_gb_fact';
SET mapred.max.split.size=128000000;
SET mapred.min.split.size=128000000;
SET mapred.min.split.size.per.node=128000000;
SET mapred.min.split.size.per.rack=128000000;
SET hive.exec.reducers.bytes.per.reducer = 128000000;
SET hive.merge.mapfiles=true;
SET hive.merge.mapredfiles= true;
SET hive.merge.size.per.task=256000000;
SET hive.exec.parallel = true; 
set mapred.job.queue.name=root.ai.offline;

set hive.support.concurrency=false;

set hive.auto.convert.join=false;
USE  dw_gearbest_recommend;
----万能接口
CREATE TABLE IF NOT EXISTS apl_sale_top100_fact(
	good_sn            string     COMMENT '推荐商品SKU',
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
COMMENT '网站热销top100'
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\u0001'                                                                                   
LINES TERMINATED BY '\n'                                                                                          
STORED AS TEXTFILE;

INSERT OVERWRITE TABLE apl_sale_top100_fact
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
					goods_spu,
					sellcount
				FROM
					gb_paltform_hotsell_top100
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


CREATE TABLE IF NOT EXISTS apl_sale_top100_pipeline_fact(
	good_sn            string     COMMENT '推荐商品SKU',
	webGoodSn          string     COMMENT '商品webSku',
	pipeline_code      string     COMMENT '渠道编码',
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
COMMENT '网站热销top100'
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\u0001'                                                                                   
LINES TERMINATED BY '\n'                                                                                          
STORED AS TEXTFILE;



INSERT OVERWRITE TABLE apl_sale_top100_pipeline_fact
SELECT
	good_sn,
	goods_web_sku,
	pipeline_code,
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
		pipeline_code,
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
		ROW_NUMBER() OVER(PARTITION BY good_sn,pipeline_code,good_title,lang ORDER BY shop_price ASC) AS flag
	FROM(
		SELECT
			good_sn,
			goods_web_sku,
			pipeline_code,
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
				t1.pipeline_code,
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
					pipeline_code,
					sellcount
				FROM
					gb_paltform_hotsell_top100
				) t1
			JOIN
				goods_info_mid5 t2
			ON
				t1.goods_spu = t2.goods_spu
			JOIN
				goods_info_result_rec t3
			ON
				t2.good_sn = t3.good_sn
			)tmp
		GROUP BY
			good_sn,
			goods_web_sku,
			pipeline_code,
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


CREATE TABLE IF NOT EXISTS apl_topsale_category50_fact(
	category_id        string     COMMENT '商品分类ID',
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
COMMENT '商品分类销量top50'
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\u0001'                                                                                   
LINES TERMINATED BY '\n'                                                                                          
STORED AS TEXTFILE;




INSERT OVERWRITE TABLE apl_topsale_category50_fact
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
					category_id,
					sellcount
				FROM
					goods_spu_hotsell_15days
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



CREATE TABLE IF NOT EXISTS apl_top10_category_fact(
	category_id        string     COMMENT '商品分类ID',
	good_sn            string     COMMENT '商品SKU',
	webGoodSn          string     COMMENT '商品webSku',
	pipeline_code      string     COMMENT '渠道编码',
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
COMMENT '商品分类销量top50'
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\u0001'                                                                                   
LINES TERMINATED BY '\n'                                                                                          
STORED AS TEXTFILE;




INSERT OVERWRITE TABLE apl_top10_category_fact
SELECT
	category_id,
	good_sn,
	goods_web_sku,
	pipeline_code,
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
		pipeline_code,
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
			pipeline_code,
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
				t1.pipeline_code,
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
					node1 category_id,
					pipeline_code,
					sellcount
				FROM(
					SELECT
						goods_spu,
						node1,
						pipeline_code,
						sellcount,
						ROW_NUMBER() OVER(PARTITION BY node1,pipeline_code ORDER BY sellcount desc) AS flag
					FROM(
						SELECT
							a.goods_spu,
							b.node1,
							pipeline_code,
							sellcount
						FROM
							goods_spu_hotsell_15days a
						JOIN
							apl_nodetree_gb_fact b
						ON
							a.category_id = b.cat_id
						)a
					)tmp
				WHERE
					flag < 3
				) t1
			JOIN
				goods_info_mid5 t2
			ON
				t1.goods_spu = t2.goods_spu
			JOIN
				goods_info_result_rec t3
			ON
				t2.good_sn = t3.good_sn and t3.pipeline_code = t1.pipeline_code
			)tmp
		GROUP BY
			category_id,
			good_sn,
			goods_web_sku,
			pipeline_code,
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