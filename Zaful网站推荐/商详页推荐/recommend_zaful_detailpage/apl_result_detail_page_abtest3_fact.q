--@author LIUQINGFAN
--@date 2018年5月30日 
--@desc  商品购物车页推荐位结果输出

SET mapred.job.name='apl_result_detail_page_abtest3_fact';
set mapred.job.queue.name=root.ai.offline;
SET mapred.max.split.size=128000000;
SET mapred.min.split.size=64000000;
SET mapred.min.split.size.per.node=64000000;
SET mapred.min.split.size.per.rack=64000000;
SET hive.exec.reducers.bytes.per.reducer = 64000000;
SET hive.merge.mapfiles=true;
SET hive.merge.mapredfiles= true;
SET hive.input.format=org.apache.hadoop.hive.ql.io.CombineHiveInputFormat;
SET hive.merge.size.per.task=256000000;
SET hive.exec.parallel = true;
set hive.support.concurrency=false;
USE dw_zaful_recommend;

CREATE TABLE IF NOT EXISTS result_detail_page_abtest_v4
(
	goods_sn1        string  COMMENT '商品SKU',
        platform string,
	goods_sn2        string  COMMENT '推荐商品',
	score            int     COMMENT '商品评分'
	)
COMMENT "商详页ABTEST推荐商品"
PARTITIONED BY(year string,month string,day string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\u0001' 
LINES TERMINATED BY '\n' 
STORED AS TEXTFILE;


CREATE TABLE IF NOT EXISTS apl_result_detail_page_abtest3_fact(
	goodssn1           string     COMMENT '商详页展示商品',
        platform           string     COMMENT '区分平台',
	goodssn2           string     COMMENT '推荐商品SKU',
	goodsid            int        COMMENT '商品ID',
	catid              int        COMMENT '商品分类ID',
	goodstitle         string     COMMENT '商品title',
	goodscolor         string     COMMENT '商品颜色',
	goodssize          string     COMMENT '商品尺寸',
	gridurl            string     COMMENT 'grid图url',
	pipelinecode       string     COMMENT '渠道编码',
	shopcode           string     COMMENT '店铺ID',
	webgoodSn          string     COMMENT '商品webSku',
	lang               string     COMMENT '语言',
	warecode           int        COMMENT '仓库',
	reviewcount        int        COMMENT '评论数',
	avgrate            double     COMMENT '评论分',
	shopprice          double     COMMENT '本店售价',
	favoritecount      int 	      COMMENT '收藏数量',
	goodsnum           int        COMMENT '商品库存',
	imgurl             string     COMMENT '产品图url',
	thumburl           string     COMMENT '缩略图url',
	thumbextendUrl     string     COMMENT '商品图片',
	urltitle           string     COMMENT '静态页面文件标题',
	score              int        COMMENT '排序字段'
	)
COMMENT '商详页ABTEST推荐结果'
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\u0001'                                                                                   
LINES TERMINATED BY '\n'                                                                                          
STORED AS TEXTFILE;


INSERT OVERWRITE TABLE  dw_zaful_recommend.apl_result_detail_page_abtest3_fact
SELECT
	NVL(a.goods_sn1,'')              ,
        NVL(a.platform,'')              ,
	NVL(b.goodssn,'')              ,
	NVL(b.goodsid,0)              ,
	NVL(b.catid,0)                ,
	NVL(b.goodstitle,'')           ,
	NVL(b.goodscolor,'')           ,
	NVL(b.goodssize,'')           ,
	NVL(b.gridurl,'')              ,
	NVL(b.pipelinecode,'')        ,
	NVL(b.shopcode,'')             ,
	NVL(b.webgoodSn,'')            ,
	NVL(b.lang,'en')                 ,
	NVL(b.warecode,0)             ,
	NVL(b.reviewcount,0)          ,
	NVL(b.avgrate,0)              ,
	NVL(b.shopprice,0)            ,
	NVL(b.favoritecount,0)        ,
	NVL(b.goodsnum,0)             ,
	NVL(b.imgurl,'')               ,
	NVL(b.thumburl,'')             ,
	NVL(b.thumbextendUrl,'')       ,
	NVL(b.urltitle,'')            ,
	NVL(a.score,0)
FROM
	(
		SELECT
			goods_sn1,
                        platform,
			goods_sn2,
			score
		FROM
			dw_zaful_recommend.result_detail_page_abtest_v4
		WHERE
			concat(year,month,day) = ${DATE}
		)a
    JOIN
    dw_zaful_recommend.apl_zaful_result_attr_fact b
    ON
		a.goods_sn2 = b.goodssn
    ; 