--@author XiongJun
--@date 2019年4月28日 
--@desc  gb A+标签，按照pipelineCode_lang_catid_重复曝光序号_商品序号_sku

SET mapred.job.name=label_a_plus_recommend;
SET mapred.max.split.size=128000000;
SET mapred.min.split.size=32000000;
SET mapred.min.split.size.per.node=32000000;
SET mapred.min.split.size.per.rack=32000000;
SET hive.exec.reducers.bytes.per.reducer = 128000000;
SET hive.merge.mapfiles=true;
SET hive.merge.mapredfiles= true;
SET hive.merge.size.per.task=256000000; 
SET hive.auto.convert.join = false;
SET mapred.job.queue.name=root.ai.online; 
use dw_gearbest_recommend;


--获取新品及标签，关联出渠道、语言、分类id
 INSERT overwrite TABLE dw_gearbest_recommend.gb_label_a_plus_tmp  SELECT
	m.good_sn,
	m.goods_web_sku,
	m.pipeline_code,
	m.good_title,
	m.lang,
	m.id AS categoryid,
	m.v_wh_code,
	m.total_num,
	m.avg_score,
	m.shop_price,
	m.total_favorite,
	m.stock_qty,
	m.img_url,
	m.grid_url,
	m.thumb_url,
	m.thumb_extend_url,
	m.url_title
FROM	
	dw_gearbest_recommend.goods_info_result_uniqlang_filtered m
JOIN 
    dw_gearbest_recommend.gb_label_a_plus n
ON 
    m.good_sn = n.good_sn;