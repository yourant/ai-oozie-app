--Author cuijian
--zaful首页abtest
--step 1_2 计算7天内销量TOP150
USE dw_zaful_recommend;
set mapred.job.queue.name=root.ai.offline; 
SET mapred.max.split.size=128000000;
SET mapred.min.split.size=128000000;
SET mapred.min.split.size.per.node=128000000;
SET mapred.min.split.size.per.rack=128000000;
SET hive.exec.reducers.bytes.per.reducer = 128000000;
SET hive.merge.mapfiles=true;
SET hive.merge.mapredfiles= true;
SET hive.merge.size.per.task=256000000;
SET hive.auto.convert.join=false;
SET hive.exec.parallel = true; 
set hive.support.concurrency=false;
CREATE TABLE IF NOT EXISTS dw_zaful_recommend.zaful_homepage_app_rule_abtest (
    goods_sn STRING COMMENT '商品sku',
    qty INT COMMENT '订单商品数量'
  ) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' LINES TERMINATED BY '\n' STORED AS orc;
INSERT OVERWRITE TABLE dw_zaful_recommend.zaful_homepage_app_rule_abtest
SELECT
  goods_sn,
  skucount
FROM
  (
    SELECT
      spu,
      goods_sn,
      skucount,
      row_number() OVER(
        PARTITION BY spu
        ORDER BY
          skucount DESC
      ) rankid
    FROM
      (
        SELECT
          f.goods_sn,
          substr(f.goods_sn, 1, 7) spu,
          f.skucount
        FROM
          (
            SELECT
              a.goods_sn,
              SUM(a.goods_number) skucount
            FROM
              stg.zaful_eload_order_goods a
              JOIN (
                SELECT
                  order_id
                FROM
                  stg.zaful_eload_order_info
                WHERE
                  order_status != 0
                  AND order_status != 11
                  AND add_time < UNIX_TIMESTAMP()
                  AND add_time > UNIX_TIMESTAMP(
                    DATE_SUB(
                      FROM_UNIXTIME(UNIX_TIMESTAMP(), 'yyyy-MM-dd'),
                      7
                    ),
                    'yyyy-MM-dd'
                  )
              ) e ON a.order_id = e.order_id
            GROUP BY
              a.goods_sn
          ) f
          JOIN (
            SELECT
              goods_sn
            FROM
              stg.zaful_eload_goods
            WHERE
              is_on_sale = 1
              AND goods_number > 0
          ) g ON f.goods_sn = g.goods_sn
      ) h
  ) l
WHERE
  rankid = 1
ORDER BY
  skucount
DESC
LIMIT
  150;


CREATE TABLE IF NOT EXISTS apl_result_detail_cookie_zaful_abtest_3_fact(
	goodssn            string     COMMENT '推荐商品SKU',
	goodsid            int        COMMENT '商品ID',
	catid              int        COMMENT '商品分类ID',
	goodstitle         string     COMMENT '商品title',
	goodscolor         string     COMMENT '商品颜色',
	goodssize          string     COMMENT '商品尺寸',
	gridurl            string     COMMENT 'grid图url',
	pipelinecode      string     COMMENT '渠道编码',
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
COMMENT 'APP推荐商品'
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\u0001'                                                                                   
LINES TERMINATED BY '\n'                                                                                          
STORED AS TEXTFILE;


INSERT OVERWRITE TABLE apl_result_detail_cookie_zaful_abtest_3_fact
SELECT
	goods_sn,
	goods_id,
	cat_id,
	goods_title,
	'',
	'',
	goods_grid,
	'',
	'',
	'',
	'',
	'',
	'',
	'',
	'',
	'',
	'',
	'',
	'',
	'',
	'',
	10000-score
FROM(
	SELECT
		b.goods_sn,
		b.goods_id,
		b.cat_id,
		b.goods_title,
		b.goods_grid,
		'',
		'',
		'',
		'',
		'',
		'',
		'',
		'',
		'',
		'',
		'',
		'',
		'',
		'',
		a.qty score
	FROM
		zaful_homepage_app_rule_abtest a
	JOIN
		stg.zaful_eload_goods b
	ON
		a.goods_sn = b.goods_sn
	WHERE
		b.is_on_sale = 1  AND b.is_delete = 0  AND b.goods_number > 0 
	)T
;


INSERT OVERWRITE TABLE apl_result_detail_cookie_zaful_abtest_3_fact
SELECT
	NVL(a.goodssn,'')              ,
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
	NVL(b.urltitle,'')   ,
	NVL(a.score,0)  score
FROM
	apl_result_detail_cookie_zaful_abtest_3_fact a
JOIN
	tmp.apl_zaful_result_attr_fact  b
ON
	a.goodssn = b.goodssn
GROUP BY 
    NVL(a.goodssn,'')              ,
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
	NVL(b.urltitle,'')   ,
	NVL(a.score,0)
;