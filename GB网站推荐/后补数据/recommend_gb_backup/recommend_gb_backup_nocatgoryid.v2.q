--@author zhangyuchao
--@date 2019年04月09日 
--@desc  gb推荐后台后补数据，按pipeline_lang分组，无分类，分为一二三四，每组至多30个后补商品

SET mapred.job.name=goods_info_result_backup_nocategoryid_result;
SET mapred.max.split.size=128000000;
SET mapred.min.split.size=32000000;
SET mapred.min.split.size.per.node=32000000;
SET mapred.min.split.size.per.rack=32000000;
SET hive.exec.reducers.bytes.per.reducer = 128000000;
SET hive.merge.mapfiles=true;
SET hive.merge.mapredfiles= true;
SET hive.merge.size.per.task=256000000; 
use dw_gearbest_recommend;


CREATE TABLE IF NOT EXISTS dw_gearbest_recommend.goods_info_result_backup_nocategoryid(
  `good_sn` string, 
  `goods_spu` string, 
  `goods_web_sku` string, 
  `shop_code` bigint, 
  `goods_status` int, 
  `brand_code` string, 
  `first_up_time` bigint, 
  `v_wh_code` int, 
  `shop_price` double, 
  `level_cnt` int, 
  `good_title` string, 
  `img_url` string, 
  `grid_url` string, 
  `thumb_url` string, 
  `thumb_extend_url` string, 
  `lang` string, 
  `stock_qty` bigint, 
  `avg_score` decimal(2,1), 
  `total_num` bigint, 
  `total_favorite` bigint, 
  `pipeline_code` string, 
  `url_title` string)
COMMENT '推荐位后补兜底数据无分类'
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\u0001'                                                                                   
LINES TERMINATED BY '\n'                                                                                          
STORED AS TEXTFILE;

--每个pipeline_code、lang下随机取1000个商品，无分类
 INSERT overwrite TABLE dw_gearbest_recommend.goods_info_result_backup_nocategoryid
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
	m.level_cnt,
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
			a.good_sn,
			a.goods_spu,
			a.goods_web_sku,
			a.shop_code,
			a.goods_status,
			a.brand_code,
			a.first_up_time,
			a.v_wh_code,
			a.shop_price,
			a.level_cnt,
			a.good_title,
			a.img_url,
			a.grid_url,
			a.thumb_url,
			a.thumb_extend_url,
			a.lang,
			a.stock_qty,
			a.avg_score,
			a.total_num,
			a.total_favorite,
			a.pipeline_code,
			a.url_title,
			ROW_NUMBER () OVER (
				PARTITION BY a.pipeline_code,
				a.lang
			ORDER BY
				rand()
			) AS flag
		FROM
			dw_gearbest_recommend.goods_info_result_uniqlang_filtered a
		WHERE
			a.pipeline_code IS NOT NULL
		AND a.lang IS NOT NULL
	) m
WHERE
	m.flag <= 1000;

--相同spu只能取一个sku
INSERT overwrite TABLE dw_gearbest_recommend.goods_info_result_backup_nocategoryid SELECT
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
	dw_gearbest_recommend.goods_info_result_backup_nocategoryid m
GROUP BY
m.pipeline_code,
m.lang,
m.goods_spu;

CREATE TABLE IF NOT EXISTS dw_gearbest_recommend.goods_backup_nocategoryid_result(
  `good_sn` string, 
  `webgoodsn` string, 
  `pipeline_code` string, 
  `goodstitle` string, 
  `lang` string, 
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
COMMENT '推荐位后补兜底数据最后无分类的汇总数据'
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\u0001'                                                                                   
LINES TERMINATED BY '\n'                                                                                          
STORED AS TEXTFILE;

INSERT overwrite TABLE dw_gearbest_recommend.goods_backup_nocategoryid_result
	select 
		  b.good_sn, 
		  b.goods_web_sku, 
		  b.pipeline_code,
		  b.good_title,
		  b.lang,
		  b.v_wh_code,
		  b.total_num,
		  b.avg_score, 
		  b.shop_price, 
		  b.total_favorite,
		  b.stock_qty, 
		  b.img_url, 
		  b.grid_url, 
		  b.thumb_url, 
		  b.thumb_extend_url, 
		  b.url_title
    FROM
      dw_gearbest_recommend.goods_info_result_backup_nocategoryid b;
