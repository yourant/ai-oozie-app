--@author LIUQINGFAN
--@date 2018年8月6日 
--@desc  商详页ABTEST推荐位结果输出

SET mapred.job.name='apl_result_gb_email_create_fact';

set mapred.job.queue.name=root.ai.online; 

set mapred.max.split.size=100000000;
set mapred.min.split.size.per.node=100000000;
set mapred.min.split.size.per.rack=100000000;
set hive.auto.convert.join = false; 
set hive.exec.reducers.bytes.per.reducer=500000000;
set hive.groupby.skewindata=true;

USE dw_gearbest_recommend;

CREATE TABLE IF NOT EXISTS result_gb_email_create
(
	goods_sn1        string  COMMENT '商品sku',
	goods_sn2         string  COMMENT '推荐商品',
	score            int     COMMENT '商品评分'
	)
COMMENT "GB邮件算法结果推荐商品"
PARTITIONED BY(year string,month string,day string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\u0001' 
LINES TERMINATED BY '\n' 
STORED AS TEXTFILE;


CREATE TABLE IF NOT EXISTS result_gb_email_payed
(
	goods_sn1        string  COMMENT '商品sku',
	goods_sn2         string  COMMENT '推荐商品',
	score            int     COMMENT '商品评分'
	)
COMMENT "GB邮件算法结果推荐商品"
PARTITIONED BY(year string,month string,day string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\u0001' 
LINES TERMINATED BY '\n' 
STORED AS TEXTFILE;

CREATE TABLE IF NOT EXISTS result_gb_email_promptpay
(
	goods_sn1        string  COMMENT '商品sku',
	goods_sn2         string  COMMENT '推荐商品',
	score            int     COMMENT '商品评分'
	)
COMMENT "GB邮件算法结果推荐商品"
PARTITIONED BY(year string,month string,day string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\u0001' 
LINES TERMINATED BY '\n' 
STORED AS TEXTFILE;

CREATE TABLE IF NOT EXISTS apl_result_gb_email_create_fact(
	goods_sn1          string     COMMENT '主商品SKU',
	goods_sn2          string     COMMENT '推荐商品SKU',
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
	score              int        COMMENT '推荐顺序'
	)
COMMENT 'GB邮件算法结果推荐商品'
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\u0001'                                                                                   
LINES TERMINATED BY '\n'                                                                                          
STORED AS TEXTFILE;	

INSERT OVERWRITE TABLE apl_result_gb_email_create_fact
SELECT
	NVL(goods_sn1,''),
	NVL(goods_sn2,''),
	NVL(pipeline_code,''),
	NVL(goods_web_sku,''),
	NVL(good_title,''),
	NVL(lang,''),
	NVL(v_wh_code,0),
	NVL(total_num,0),
	NVL(avg_score,0),
	NVL(shop_price,0),
	NVL(total_favorite,0),
	NVL(stock_qty,0),
	NVL(img_url,''),
	NVL(grid_url,''),
	NVL(thumb_url,''),
	NVL(thumb_extend_url,''),
	NVL(url_title,''),
	NVL(score,0) score
FROM(
	SELECT
		t1.goods_sn1,
		t1.goods_sn2,
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
		t1.score
	FROM(
		SELECT
			goods_sn1,
			goods_sn2,
			score
		FROM
			result_gb_email_create
		WHERE
			concat(year,month,day) = ${DATE}
		) t1
	JOIN
		(
	SELECT
		n.*
	FROM
		dw_gearbest_recommend.goods_info_result_uniq n
	JOIN dw_gearbest_recommend.pipeline_language_map x ON n.pipeline_code = x.pipeline_code
	AND n.lang = x.lang
) t3
	ON
		t1.goods_sn2 = t3.good_sn
)tmp;








	
	
	
	

	