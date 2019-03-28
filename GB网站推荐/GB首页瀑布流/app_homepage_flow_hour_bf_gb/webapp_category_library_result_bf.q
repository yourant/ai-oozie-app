--@author LIUQINGFAN
--@date 2018年8月6日 
--@desc  瀑布流置顶商品

SET mapred.job.name='webapp_category_library_result';

set mapred.job.queue.name=root.ai.online;

SET mapred.max.split.size=128000000;
SET mapred.min.split.size=128000000;
SET mapred.min.split.size.per.node=128000000;
SET mapred.min.split.size.per.rack=128000000;
SET hive.exec.reducers.bytes.per.reducer = 128000000;
SET hive.merge.mapfiles=true;
SET hive.merge.mapredfiles= true;
SET hive.merge.size.per.task=256000000;
SET hive.exec.parallel = true; 
SET hive.auto.convert.join=false;

USE dw_gearbest_recommend;


CREATE TABLE IF NOT EXISTS `top_goods_settings`(
  `id` int, 
  `position` string, 
  `goods_sku` string, 
  `keywords` string, 
  `cat_id` string, 
  `is_show` tinyint, 
  `start_time` int, 
  `end_time` int, 
  `create_time` int, 
  `update_time` int, 
  `pipeline_code` string, 
  `lang` string, 
  `platform` tinyint, 
  `copy_id` int, 
  `tab_id` int)
COMMENT ''
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\u0001'                                                                                   
LINES TERMINATED BY '\n'                                                                                          
STORED AS TEXTFILE location '/user/hive/warehouse/dw_gearbest_recommend.db/top_goods_settings';;


INSERT OVERWRITE TABLE webapp_category_library_info_order
SELECT
	goods_sn,
	vcode,
	id, 
	start_time,
	end_time,
	create_time,
	update_time,
	pipeline_code,
	lang,
	platform,
	tab_id,
	flag1
from(
	SELECT
		goods_sn,
		vcode,
		id, 
		start_time,
		end_time,
		create_time,
		update_time,
		pipeline_code,
		lang,
		platform,
		tab_id,
		flag1,
		ROW_NUMBER() OVER(PARTITION BY tab_id,pipeline_code,lang,platform,goods_sn ORDER BY update_time desc ) AS flag
	FROM(
		select 
			regexp_extract(browse_product,'(.*)_(.*)',1) goods_sn,
			regexp_extract(browse_product,'(.*)_(.*)',2) vcode ,
			id, 
			start_time,
			end_time,
			create_time,
			update_time,
			pipeline_code,
			lang,
			platform,
			tab_id,
			ROW_NUMBER() OVER(PARTITION BY 1 ORDER BY 1 ) AS flag1
		from 
			(select 
			id, 
			start_time,
			end_time,
			create_time,
			update_time,
			pipeline_code,
			lang,
			platform,
			tab_id,
			browse_product 
		from top_goods_settings  
				lateral view explode(split(goods_sku, ',')) myTable as browse_product where position in  ('webapp_category_library','index_recommend')  and is_show = 1 and current_timestamp() between start_time and end_time 
			)a
		)tmp
	)tmp
WHERE
	flag = 1
;



CREATE TABLE IF NOT EXISTS webapp_category_library_result_bf(
	tab_id             int        ,
	good_sn            string     COMMENT '推荐商品SKU',
	pipeline_code      string     COMMENT '渠道编码',
	webGoodSn          string     COMMENT '商品webSku',
	goodsTitle         string     COMMENT '商品title',
	catid              int        COMMENT '商品分类ID',
	lang               string     COMMENT '语言',
	wareCode           int        COMMENT '仓库',
	reviewCount        int        COMMENT '评论数',
	avgRate            double     COMMENT '评论分',
	shopPrice          double     COMMENT '本店售价',
	favoriteCount      int 	      COMMENT '收藏数量',
	goodsNum           int        COMMENT '商品库存',
	originalUrl        string     COMMENT '商品原图',
	imgUrl             string     COMMENT '产品图url',
	gridUrl            string     COMMENT 'grid图url',
	thumbUrl           string     COMMENT '缩略图url',
	thumbExtendUrl     string     COMMENT '商品图片',
	url_title          string     COMMENT '静态页面文件标题',
	platform           int,
	score               int
	)
COMMENT '底层中间表'
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\u0001'                                                                                   
LINES TERMINATED BY '\n'                                                                                          
STORED AS TEXTFILE;

INSERT OVERWRITE TABLE webapp_category_library_result_bf
SELECT
	t1.tab_id,
	t1.good_sn,
	t1.pipeline_code,
	t1.goods_web_sku,
	t1.good_title,
	t1.cat_id,
	t1.lang,
	t1.v_wh_code warecode,
	t1.total_num,
	t1.avg_score,
	t1.shop_price,
	t1.total_favorite,
	t1.stock_qty,
	t2.original_url,
	t1.img_url,
	t1.grid_url,
	t1.thumb_url,
	t1.thumb_extend_url,
	t1.url_title,
	t1.platform,
	t1.flags score
FROM(
	SELECT
		t1.tab_id,
		t3.good_sn,
		t3.pipeline_code,
		t3.goods_web_sku,
		t3.good_title,
		t3.id cat_id,
		t3.lang,
		t1.vcode as v_wh_code,
		t3.total_num,
		t3.avg_score,
		t3.shop_price,
		t3.total_favorite,
		t3.stock_qty,
		t3.img_url,
		t3.grid_url,
		t3.thumb_url,
		t3.thumb_extend_url,
		t3.url_title,
		t1.platform,
		ROW_NUMBER() OVER(PARTITION BY tab_id,t1.pipeline_code,t1.lang,platform ORDER BY flag1 desc ) AS flags
	FROM
		webapp_category_library_info_order t1
	JOIN
		goods_info_result_uniqlang t3
	ON
		t1.pipeline_code = t3.pipeline_code AND t1.lang = t3.lang and t1.goods_sn = t3.good_sn 
		--and t1.vcode = t3.v_wh_code
	)t1
JOIN(
	SELECT
		good_sn,
		original_url
	FROM
		goods_info_mid2
	GROUP BY
		good_sn,
		original_url
	)t2
ON
	t1.good_sn = t2.good_sn;
	

	
	
