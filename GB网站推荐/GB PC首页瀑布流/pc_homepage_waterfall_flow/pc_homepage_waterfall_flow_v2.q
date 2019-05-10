--@author xiongjun
--@date 2019年5月6日 
--@desc  GB PC首页瀑布流新规则v2，在原有基础上修改

SET mapred.job.name=pc_homepage_waterfall_flow;
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
set hive.auto.convert.join=false;

--待过滤商品集
CREATE TABLE IF NOT EXISTS dw_gearbest_recommend.sku_not_sale_pc_new
(
	good_sn      string 
	)
COMMENT "PC瀑布流禁售商品"
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\u0001' 
LINES TERMINATED BY '\n' 
STORED AS TEXTFILE;

--待过滤商品集（禁售分类，BF,DA商品）
INSERT OVERWRITE TABLE dw_gearbest_recommend.sku_not_sale_pc_new SELECT
	good_sn
FROM
	dw_gearbest_recommend.goods_info_mid1
WHERE
	(
		level_3 IN (11372, 11380, 12056)
	)
OR (
	level_2 IN (
		11502,
		11281,
		12433,
		12181,
		11546
	)
)
UNION ALL
	SELECT
		good_sn
	FROM
		ods.ods_m_gearbest_base_goods_goods    --BF商品
	WHERE
		dt = '${DATE}'
	AND recommended_level = 14
	UNION ALL
		SELECT
			good_sn
		FROM
			ods.ods_m_gearbest_base_goods_new_goods_label    --DA商品
		WHERE
			dt = '${DATE}'
		AND label_code = '00000238'
;


--曝光数
CREATE TABLE IF NOT EXISTS dw_gearbest_recommend.goods_exp_num (
    good_sn           string   COMMENT 'SKU', 
    exp_num            int   COMMENT '曝光数'
	)
COMMENT "GB商品曝光数"
PARTITIONED BY (`date` string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\u0001' 
LINES TERMINATED BY '\n' 
STORED AS TEXTFILE;

--GB 商品曝光数
INSERT OVERWRITE TABLE dw_gearbest_recommend.goods_exp_num partition (`date` = '${DATE}')
SELECT
	t.goods_sn as good_sn,
    count(*) as exp_num 
FROM(
	SELECT
		get_json_object(a.sub_event_field,'$.sku') as goods_sn,
        b.cookie_id
	FROM(
		SELECT
			log_id,
			sub_event_field
		FROM
			ods.ods_pc_burial_log_ubcta
		WHERE
			concat(year,month,day) = '${DATE}'
            and site='gearbest'
		) a
	JOIN(
		SELECT
			log_id,
            cookie_id
		FROM
			ods.ods_pc_burial_log
		WHERE
		    concat(year,month,day) = '${DATE}'
            and site='gearbest'
            AND behaviour_type = 'ie' AND  sub_event_field != ''
		) b
	ON
		a.log_id = b.log_id
	) t
GROUP BY t.goods_sn
;

--商品每日毛利率
CREATE TABLE IF NOT EXISTS dw_gearbest_recommend.goods_gross_profit (
    good_sn           string   COMMENT 'SKU', 
    profit            double   COMMENT '毛利率'
	)
COMMENT "GB商品毛利率"
PARTITIONED BY (`date` string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\u0001' 
LINES TERMINATED BY '\n' 
STORED AS TEXTFILE;

--计算商品每日毛利率
INSERT OVERWRITE TABLE dw_gearbest_recommend.goods_gross_profit  partition (`date` = '${DATE}')
SELECT
	t1.good_sn,
    CASE WHEN (t0.price - t1.ship_price / t1.exchange_rate) <= 0 THEN -1
    ELSE (t0.price - (t1.ship_price + t1.ship_fee) / t1.exchange_rate) / abs(t0.price - t1.ship_price / t1.exchange_rate)
    END AS profit
FROM
	(
		SELECT
	    x.price,
		x.price_md5,
		x.warehouse_code,
		x.goods_sn
		FROM
		(
			SELECT
				price,
				price_md5,
				warehouse_code,
				goods_sn,
				ROW_NUMBER () OVER (
				PARTITION BY
					goods_sn
				ORDER BY
					price ASC
			) AS flag
		FROM
			ods.ods_m_gearbest_gb_order_order_goods
		WHERE
			dt = '${DATE}'
		) x
	WHERE
	x.flag = 1
	) t0
JOIN (
	SELECT
		ship_price,
		exchange_rate,
		good_sn,
		price_md5,
		wh_code,
        ship_fee
	FROM
		ods.ods_m_gearbest_goods_price_factor
	WHERE
		dt = '${DATE}'
	GROUP BY 
		ship_price,
		exchange_rate,
		good_sn,
		price_md5,
		wh_code,
        ship_fee
) t1 ON t0.goods_sn = t1.good_sn
AND t0.price_md5 = t1.price_md5
AND t0.warehouse_code = t1.wh_code
;

--商品每日销量、销售额、下单数
CREATE TABLE IF NOT EXISTS dw_gearbest_recommend.goods_order_his (
    good_sn           string   COMMENT 'SKU', 
    pipeline_code     string   COMMENT '国家站',
    goods_num            int   COMMENT '销量',
    order_num            int   COMMENT '下单数',
    pay_amount         double  COMMENT '销售额'
	)
COMMENT "GB商品订单相关指标"
PARTITIONED BY (`date` string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\u0001' 
LINES TERMINATED BY '\n' 
STORED AS TEXTFILE;




--分渠道计算每日商品销量、销售额、下单数
INSERT OVERWRITE TABLE dw_gearbest_recommend.goods_order_his partition (`date` = '${DATE}')
SELECT
  p.goods_sn,
  x.pipeline_code,
  sum(p.qty) as goods_num,
  sum(1) as order_num,
  sum(p.price * p.qty) as pay_amount
FROM
  (
    SELECT
      order_sn,
      pipeline_code
    FROM
      ods.ods_m_gearbest_gb_order_order_info
    WHERE
      dt = '${DATE}' and
      from_unixtime(created_time + 8*3600, 'yyyyMMdd') = '${DATE}'
      and order_status in ('0','6')
  ) x
JOIN 
(
    SELECT qty,price,order_sn,goods_sn from ods.ods_m_gearbest_gb_order_order_goods
    WHERE
      dt = '${DATE}'
) p ON x.order_sn = p.order_sn
group by p.goods_sn,x.pipeline_code
;


--近7天毛利率
CREATE TABLE IF NOT EXISTS dw_gearbest_recommend.goods_gross_profit_tmp (
    good_sn           string   COMMENT 'SKU', 
    profit            double   COMMENT '毛利率'
	)
COMMENT "GB商品毛利率"
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\u0001' 
LINES TERMINATED BY '\n' 
STORED AS TEXTFILE;

--近7天毛利率
INSERT OVERWRITE TABLE dw_gearbest_recommend.goods_gross_profit_tmp SELECT
	good_sn,
	sum(profit) / 7 AS profit
FROM
	dw_gearbest_recommend.goods_gross_profit
WHERE
	date BETWEEN '${DAYWEEK}'
AND '${DATE}'
GROUP BY
	good_sn
	;

--近七天销量、下单量、销售额、日均销量、日均销售额
CREATE TABLE IF NOT EXISTS dw_gearbest_recommend.goods_order_7_tmp (
    good_sn           string   COMMENT 'SKU', 
    pipeline_code     string   COMMENT '国家站',
    goods_num7          double   COMMENT '近七天销量',
    order_num7          double   COMMENT '近七天下单量',
    pay_amount7         double  COMMENT '近七天销售额',
    goods_num7_avg         double  COMMENT '近七天日均销量',
    pay_amount7_avg         double  COMMENT '近七天日均销售额',
    goods_num7_atOne         double  COMMENT '近七天归一化日均销量',
    pay_amount7_atOne         double  COMMENT '近七天归一化日均销售额'
	)
COMMENT "GB商品订单近七天相关指标"
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\u0001' 
LINES TERMINATED BY '\n' 
STORED AS TEXTFILE;
--近七天销量、下单量、销售额、日均销量、日均销售额
INSERT OVERWRITE TABLE dw_gearbest_recommend.goods_order_7_tmp SELECT
	good_sn,
	pipeline_code,
	sum(goods_num) AS goods_num7,
	SUM(order_num) AS order_num7,
	SUM(pay_amount) AS pay_amount7,
	sum(goods_num) / 7 AS goods_num7_avg,
    SUM(pay_amount) / 7 AS pay_amount7_avg,
	0,
	0
FROM
	dw_gearbest_recommend.goods_order_his
GROUP BY
	good_sn,
	pipeline_code
;

--归一化处理，并过滤禁售商品
 INSERT OVERWRITE TABLE dw_gearbest_recommend.goods_order_7_tmp SELECT
	a.good_sn,
	a.pipeline_code,
	a.goods_num7,
	a.order_num7,
	a.pay_amount7,
	a.goods_num7_avg,
	a.pay_amount7_avg,
	(a.goods_num7_avg - min(a.goods_num7_avg) OVER(partition by a.pipeline_code)) /(max(a.goods_num7_avg) OVER(partition by a.pipeline_code)-min(a.goods_num7_avg) OVER(partition by a.pipeline_code)),
	(a.pay_amount7_avg - min(a.pay_amount7_avg) OVER(partition by a.pipeline_code))/(max(a.pay_amount7_avg) OVER(partition by a.pipeline_code)-min(a.pay_amount7_avg) OVER(partition by a.pipeline_code))
FROM
	dw_gearbest_recommend.goods_order_7_tmp a
	left join dw_gearbest_recommend.sku_not_sale_pc_new b
	on a.good_sn=b.good_sn
	where b.good_sn is null
	;	


--候选商品集排序
CREATE TABLE IF NOT EXISTS dw_gearbest_recommend.goods_rec_rank_all (
    good_sn           string   COMMENT 'SKU',
    pipeline_code     string   COMMENT '国家站',
    trending          double   COMMENT '销量增长趋势',
    conversion        double   COMMENT '转化率',
    goods_num7_atOne         double  COMMENT '近七天归一化日均销量',
    pay_amount7_atOne         double  COMMENT '近七天归一化日均销售额',
    goods_num7_avg         double  COMMENT '近七天日均销量',
    pay_amount7_avg         double  COMMENT '近七天日均销售额',
    profit         double  COMMENT '近七天日均毛利率',
    score             double      COMMENT '分数',
    flag             int      COMMENT '顺序'
	)
COMMENT "候选商品集排序"
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\u0001' 
LINES TERMINATED BY '\n' 
STORED AS TEXTFILE;


--候选商品集排序
INSERT OVERWRITE TABLE dw_gearbest_recommend.goods_rec_rank_all
SELECT
	x.good_sn,
	x.pipeline_code,
    x.trending,
    x.conversion,
	x.goods_num7_atOne,
	x.pay_amount7_atOne,
	x.goods_num7_avg,
	x.pay_amount7_avg,
	x.profit,
	x.score,
	ROW_NUMBER () OVER (
		PARTITION BY x.pipeline_code
	ORDER BY
		x.score DESC
	)
FROM
	(
		SELECT
			n.good_sn,
			n.pipeline_code,
            (NVL(m.goods_num,0) - n.goods_num7_avg) / n.goods_num7_avg AS trending,
            n.order_num7 / t.exp_num7 AS conversion,
			n.goods_num7_atOne,
			n.pay_amount7_atOne,
			n.goods_num7_avg,
			n.pay_amount7_avg,
			NVL(h.profit,0.0) AS profit,
			0.2 * (
				(NVL(m.goods_num,0) - n.goods_num7_avg) / n.goods_num7_avg
			) 
			+ 0.3 * (n.order_num7 / t.exp_num7) 
			+ 0.2 * n.goods_num7_atOne
			+ 0.2 * n.pay_amount7_atOne 
			+ 0.1 * NVL(h.profit,0.0) AS score
		FROM
			dw_gearbest_recommend.goods_order_7_tmp n
		LEFT JOIN (
			SELECT
				good_sn,
				pipeline_code,
				goods_num
			FROM
				dw_gearbest_recommend.goods_order_his
			WHERE
				date = '${DATE}'
		) m ON m.good_sn = n.good_sn
		AND m.pipeline_code = n.pipeline_code
		JOIN (
			SELECT
				good_sn,
				SUM(exp_num) AS exp_num7
			FROM
				dw_gearbest_recommend.goods_exp_num
			WHERE
				date BETWEEN '${DAYWEEK}'
			AND '${DATE}'
			GROUP BY
				good_sn
		) t ON n.good_sn = t.good_sn
		LEFT JOIN dw_gearbest_recommend.goods_gross_profit_tmp h
		ON n.good_sn = h.good_sn
	) x
;

--满足条件的商品集，条件：近7天日均销量>1或近7天日均销售额>$20 且 近7天日均毛利率>10%
CREATE TABLE IF NOT EXISTS dw_gearbest_recommend.goods_res_recall (
    good_sn           string   COMMENT 'SKU',
    pipeline_code     string   COMMENT '国家站',
    score             int      COMMENT '顺序'
	)
COMMENT "推荐商品集"
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\u0001' 
LINES TERMINATED BY '\n' 
STORED AS TEXTFILE;

--同时满足以上两个条件的商品集
INSERT OVERWRITE TABLE dw_gearbest_recommend.goods_res_recall SELECT
	t.good_sn,
	t.pipeline_code,
	ROW_NUMBER () OVER (
		PARTITION BY t.pipeline_code
		ORDER BY
			t.score DESC
	)
FROM
	dw_gearbest_recommend.goods_rec_rank_all t
WHERE
	(t.goods_num7_avg > 1 or t.pay_amount7_avg > 20) AND t.profit > 0.1
;

--未入选商品集
CREATE TABLE IF NOT EXISTS dw_gearbest_recommend.goods_res_bak (
    good_sn           string   COMMENT 'SKU',
    pipeline_code     string   COMMENT '国家站',
    score             int      COMMENT '顺序'
	)
COMMENT "推荐商品集长尾后补"
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\u0001' 
LINES TERMINATED BY '\n' 
STORED AS TEXTFILE;

--未入选商品集，保留1000个作为长尾后补
INSERT OVERWRITE TABLE dw_gearbest_recommend.goods_res_bak SELECT
	t.good_sn,
	t.pipeline_code,
	ROW_NUMBER () OVER (
		PARTITION BY t.pipeline_code
		ORDER BY
			t.score DESC
	) + 1000
FROM
	dw_gearbest_recommend.goods_rec_rank_all t
WHERE
	(t.goods_num7_avg <= 1  AND t.pay_amount7_avg <= 20) or t.profit <= 0.1
AND t.score <= 2000
;


--有些国家站长尾后补商品也不足500个，从商品池取商品补足
CREATE TABLE IF NOT EXISTS dw_gearbest_recommend.goods_pipeline_bak (
    good_sn           string   COMMENT 'SKU',
    pipeline_code     string   COMMENT '国家站',
    score             int      COMMENT '顺序'
	)
COMMENT "推荐商品集长尾后补补充商品"
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\u0001' 
LINES TERMINATED BY '\n' 
STORED AS TEXTFILE;

--每个国家站下随机取2000个sku，作为最终长尾后补商品的补足
INSERT OVERWRITE TABLE dw_gearbest_recommend.goods_pipeline_bak SELECT
	m.good_sn,
	m.pipeline_code,
	2000      
FROM
	(
		SELECT
			a.good_sn,
			a.pipeline_code,
			ROW_NUMBER () OVER (
				PARTITION BY a.pipeline_code
			ORDER BY
				rand()
			) AS flag
		FROM
			dw_gearbest_recommend.goods_info_result_uniqlang a
            WHERE
					a.good_sn NOT IN (
						SELECT
							t.good_sn
						FROM
							dw_gearbest_recommend.sku_not_sale_pc_new t
					) 
	) m
WHERE
	m.flag <= 2000
;


--最终结果汇总
CREATE TABLE IF NOT EXISTS dw_gearbest_recommend.pc_homepage_waterfall_flow(
	tab_id             int        COMMENT '馆区ID,PC默认为0',
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
	platform           int        COMMENT '平台，PC默认为1',
	score              int        COMMENT '结果排序，从小到大'
	) 
COMMENT 'GB PC首页瀑布流推荐结果'
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\u0001'                                                                                   
LINES TERMINATED BY '\n'                                                                                          
STORED AS TEXTFILE;


--GB商品原图
CREATE TABLE IF NOT EXISTS dw_gearbest_recommend.goods_originalurl(
    good_sn            string     COMMENT 'SKU',
    originalurl        string     COMMENT '商品原图')
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\u0001' 
LINES TERMINATED BY '\n' 
STORED AS TEXTFILE;

--GB商品原图
INSERT OVERWRITE TABLE dw_gearbest_recommend.goods_originalurl
SELECT
		good_sn,
		original_url
	FROM
		dw_gearbest_recommend.goods_info_mid2
	GROUP BY
		good_sn,
		original_url
;

--最终结果汇总，过滤同款spu，按国家站打散，召回商品排在前面，后补商品排序分+1000排在后面.
--随机打散影响了之前的线性排序，APP 瀑布流和PC 瀑布流都有这样的问题，可视业务需求取消打散逻辑
--由于读取redis采用ZRANGEBYSCORE pc_homepage_waterfall_flow_0_GBPL_pl_1,分数重排保持连续
INSERT OVERWRITE TABLE dw_gearbest_recommend.pc_homepage_waterfall_flow SELECT
	r.tab_id,
	r.good_sn,
	r.pipeline_code,
	r.goods_web_sku,
	r.good_title,
	r.id,
	r.lang,
	r.v_wh_code,
	r.total_num,
	r.avg_score,
	r.shop_price,
	r.total_favorite,
	r.stock_qty,
	r.originalurl,
	r.img_url,
	r.grid_url,
	r.thumb_url,
	r.thumb_extend_url,
	r.url_title,
	r.platform,
	ROW_NUMBER () OVER (
		PARTITION BY r.pipeline_code,
		r.lang
	ORDER BY
		r.score ASC
	) AS score
FROM
	(
		SELECT
			0 AS tab_id,
			x.good_sn,
			x.pipeline_code,
			x.goods_web_sku,
			x.good_title,
			x.id,
			x.lang,
			x.v_wh_code,
			x.total_num,
			x.avg_score,
			x.shop_price,
			x.total_favorite,
			x.stock_qty,
			p.originalurl,
			x.img_url,
			x.grid_url,
			x.thumb_url,
			x.thumb_extend_url,
			x.url_title,
			1 AS platform,
			x.score
		FROM
			(
				SELECT
					m1.good_sn,
					m1.pipeline_code,
					n1.goods_web_sku,
					n1.good_title,
					n1.id,
					n1.lang,
					n1.v_wh_code,
					n1.total_num,
					n1.avg_score,
					n1.shop_price,
					n1.total_favorite,
					n1.stock_qty,
					n1.img_url,
					n1.grid_url,
					n1.thumb_url,
					n1.thumb_extend_url,
					n1.url_title,
					ROW_NUMBER () OVER (
						PARTITION BY n1.pipeline_code,
						n1.lang
					ORDER BY
						rand()
					) AS score,
					ROW_NUMBER () OVER (
						PARTITION BY n1.pipeline_code,
						n1.lang,
						n1.goods_spu
					ORDER BY
						n1.shop_price ASC
					) AS flag
				FROM
					dw_gearbest_recommend.goods_res_recall m1
				JOIN dw_gearbest_recommend.goods_info_result_uniqlang n1 ON m1.good_sn = n1.good_sn
				AND m1.pipeline_code = n1.pipeline_code
				UNION ALL
					SELECT
						m1.good_sn,
						m1.pipeline_code,
						n1.goods_web_sku,
						n1.good_title,
						n1.id,
						n1.lang,
						n1.v_wh_code,
						n1.total_num,
						n1.avg_score,
						n1.shop_price,
						n1.total_favorite,
						n1.stock_qty,
						n1.img_url,
						n1.grid_url,
						n1.thumb_url,
						n1.thumb_extend_url,
						n1.url_title,
						1000 + ROW_NUMBER () OVER (
							PARTITION BY n1.pipeline_code,
							n1.lang
						ORDER BY
							rand()
						) AS score,
						ROW_NUMBER () OVER (
							PARTITION BY n1.pipeline_code,
							n1.lang,
							n1.goods_spu
						ORDER BY
							n1.shop_price ASC
						) AS flag
					FROM
						dw_gearbest_recommend.goods_res_bak m1
					JOIN dw_gearbest_recommend.goods_info_result_uniqlang n1 ON m1.good_sn = n1.good_sn
					AND m1.pipeline_code = n1.pipeline_code
				UNION ALL
					SELECT
						m1.good_sn,
						m1.pipeline_code,
						n1.goods_web_sku,
						n1.good_title,
						n1.id,
						n1.lang,
						n1.v_wh_code,
						n1.total_num,
						n1.avg_score,
						n1.shop_price,
						n1.total_favorite,
						n1.stock_qty,
						n1.img_url,
						n1.grid_url,
						n1.thumb_url,
						n1.thumb_extend_url,
						n1.url_title,
						2000 + ROW_NUMBER () OVER (
							PARTITION BY n1.pipeline_code,
							n1.lang
						ORDER BY
							rand()
						) AS score,
						ROW_NUMBER () OVER (
							PARTITION BY n1.pipeline_code,
							n1.lang,
							n1.goods_spu
						ORDER BY
							n1.shop_price ASC
						) AS flag
					FROM
						dw_gearbest_recommend.goods_pipeline_bak m1
					JOIN dw_gearbest_recommend.goods_info_result_uniqlang n1 ON m1.good_sn = n1.good_sn
					AND m1.pipeline_code = n1.pipeline_code
			) x
		JOIN dw_gearbest_recommend.goods_originalurl p ON x.good_sn = p.good_sn
		WHERE
			x.flag = 1
	) r;