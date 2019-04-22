--@author ZhanRui
--@date 2019年03月0日 
--@desc  gb邮件推荐BTS算法数据关联商品信息

SET mapred.job.name=apl_result_zf_sku_remove_cate;
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


CREATE TABLE IF NOT EXISTS dw_zaful_recommend.apl_result_zf_sku_remove_cate(
	goods_sn1       	string     COMMENT '被关联的商品sku',
	goodssn             string     COMMENT '推荐商品SKU',
	goodsid             int        COMMENT '商品ID',
	catid               int        COMMENT '商品分类ID',
	goodstitle          string     COMMENT '商品title',
	gridurl             string     COMMENT 'grid图url',
	lang                string     COMMENT '语言',
	imgurl              string     COMMENT '产品图url',
	thumburl            string     COMMENT '缩略图url',
	urltitle            string     COMMENT '静态页面文件标题',
	score				int       COMMENT '排名'
	)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\u0001'                                                                                   
LINES TERMINATED BY '\n'                                                                                          
STORED AS TEXTFILE;


INSERT OVERWRITE TABLE dw_zaful_recommend.apl_result_zf_sku_remove_cate
SELECT
	NVL(a.goods_sn1,'')              ,
	NVL(b.goodssn,'')              ,
	NVL(b.goodsid,0)              ,
	NVL(b.catid,0)                ,
	NVL(b.goodstitle,'')           ,
	NVL(b.gridurl,'')              ,
	NVL(b.lang,'')                 ,
	NVL(b.imgurl,'')               ,
	NVL(b.thumburl,'')             ,
	NVL(b.urltitle,'')      ,
	NVL(a.score,0) 
FROM
	(
		SELECT
			goods_sn1,
			goods_sn2,
			score
		FROM
			dw_zaful_recommend.result_zf_sku_remove_cate
	) a
JOIN 
	tmp.apl_zaful_result_attr_fact b 
ON 
	a.goods_sn2 = b.goodssn
;