--@author LIUQINGFAN
--@date 2018年6月19日 
--@desc  标签数据更新

SET mapred.job.name='apl_result_lable_gb_fact';
set mapred.job.queue.name=root.ai.offline; 
USE   dw_gearbest_recommend;


CREATE TABLE IF NOT EXISTS apl_lable_new_fact(
	good_sn            string     COMMENT '推荐商品SKU',
	catid              int        COMMENT '商品分类ID',
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
	url_title          string     COMMENT '静态页面文件标题',
	lableid            string     COMMENT '标签ID'
	)
COMMENT '新品标签列表'
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\u0001'                                                                                   
LINES TERMINATED BY '\n'                                                                                          
STORED AS TEXTFILE;

INSERT OVERWRITE TABLE apl_lable_new_fact
SELECT
	distinct
	t1.goodsn,
	t1.catid,
	t3.pipeline_code,
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
	t3.url_title,
	t1.lableid
FROM(
	SELECT  
		goodsn   ,    
		catid ,
		lable lableid
	FROM(
		SELECT   
			goodsn   ,    
			catid    ,       
			lable ,
			ROW_NUMBER() OVER(PARTITION BY catid ORDER BY rand()) AS flag
		FROM(
			SELECT
				a.id,
				a.good_sn goodsn,
				a.lable_id lable,
				b.id catid
			FROM
				data_input_lable_input a
			JOIN
				goods_info_result_rec b
			ON
				a.good_sn = b.good_sn
			WHERE
				(a.pdate=20181122 and a.lable=1 and a.lable_id=1) or (a.pdate=20180813 and a.lable=1 and a.lable_id=1) or (a.pdate=20180821 and a.lable=1 and a.lable_id=1) or (a.pdate=20181007 and a.lable=1 and a.lable_id=1)or (a.pdate=20181010 and a.lable=1 and a.lable_id=1)
			)a
		)tmp
	WHERE
		flag < 51
	)t1
JOIN
	goods_info_result_uniqlang t3
ON
	t1.goodsn = t3.good_sn 
WHERE
	good_title != '';
	




CREATE TABLE IF NOT EXISTS apl_lable_money_fact(
	good_sn            string     COMMENT '推荐商品SKU',
	catid              int        COMMENT '商品分类ID',
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
	url_title          string     COMMENT '静态页面文件标题',
	lableid            string     COMMENT '标签ID'
	)
COMMENT '赚钱标签列表'
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\u0001'                                                                                   
LINES TERMINATED BY '\n'                                                                                          
STORED AS TEXTFILE;

INSERT OVERWRITE TABLE apl_lable_money_fact
SELECT
	distinct
	t1.goodsn,
	t1.catid,
	t3.pipeline_code,
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
	t3.url_title,
	t1.lableid
FROM(
	SELECT  
		goodsn   ,    
		catid ,
		lable lableid
	FROM(
		SELECT   
			goodsn   ,    
			catid    ,       
			lable ,
			ROW_NUMBER() OVER(PARTITION BY catid ORDER BY rand()) AS flag
		FROM(
			SELECT
				a.id,
				a.good_sn goodsn,
				a.lable_id lable,
				b.id catid
			FROM
				data_input_lable_input a
			JOIN
				goods_info_result_rec b
			ON
				a.good_sn = b.good_sn
			WHERE
				(a.pdate=20181122 and a.lable=1 and a.lable_id=2) or (a.pdate=20180813 and a.lable=1 and a.lable_id=2) or (a.pdate=20180821 and a.lable=1 and a.lable_id=2) or (a.pdate=20181007 and a.lable=1 and a.lable_id=2) or (a.pdate=20181010 and a.lable=1 and a.lable_id=2)
			)a
		)tmp
	WHERE
		flag < 51
	)t1
JOIN
	goods_info_result_uniqlang t3
ON
	t1.goodsn = t3.good_sn 
WHERE
	good_title != '';
	
	
	

	




