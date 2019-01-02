--@author LIUQINGFAN
--@date 2018年6月19日 
--@desc  标签数据更新

SET mapred.job.name='apl_lable_app_fact';
set mapred.job.queue.name=root.ai.online; 
USE   dw_gearbest_recommend;


CREATE TABLE IF NOT EXISTS data_input_lable_input
(
	id        int      COMMENT '序号',
	good_sn   string   COMMENT '商品SKU',
	lable_id  int      COMMENT '标签标识',
	name      int      COMMENT '工号'
	)
COMMENT "商品销售状态信息"
PARTITIONED BY (pdate string,lable string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' 
LINES TERMINATED BY '\n' 
STORED AS TEXTFILE;


	
CREATE TABLE IF NOT EXISTS apl_lable_app_fact(
	id                 int        COMMENT '随机序号',
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
COMMENT 'APP标签列表'
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\u0001'                                                                                   
LINES TERMINATED BY '\n'                                                                                          
STORED AS TEXTFILE;

INSERT OVERWRITE TABLE apl_lable_app_fact
SELECT
	t1.id/24,
	t1.goodsn,
	t3.id,
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
	t1.lable
FROM(
	SELECT
		a.id,
		a.good_sn goodsn,
		a.lable_id lable
	FROM
		data_input_lable_input a
	WHERE
		a.pdate=20180813 and a.lable=3 and a.lable_id=3
	)t1
JOIN
	goods_info_result_rec t3
ON
	t1.goodsn = t3.good_sn
WHERE
	good_title != '';