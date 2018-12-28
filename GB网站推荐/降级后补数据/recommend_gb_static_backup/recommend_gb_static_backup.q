--@author ZhanRui
--@date 2018年10月24日 
--@desc  gb推荐后台降级方案后补数据，按pipeline_lang分组，每组至多50个后补商品

SET mapred.job.name=goods_static_backup_result;
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


--按pipeline_code/lang 做降级后补数据，优先取全平台热销数据
INSERT overwrite TABLE  dw_gearbest_recommend.goods_static_backup_mid
SELECT
	x.pipeline_code,
	x.goods_spu,
	x.lang
FROM
	(
		SELECT
			t.pipeline_code,
			t.goods_spu,
			t.lang,
			ROW_NUMBER () OVER (
				PARTITION BY t.pipeline_code,
				t.lang
			ORDER BY
				t.rank
			) AS flag
		FROM
			(
				SELECT
					n.pipeline_code,
					n.goods_spu,
					n.lang,
					1 AS rank
				FROM
					dw_gearbest_recommend.goods_backup_mid n
				GROUP BY
					n.pipeline_code,
					n.goods_spu,
					n.lang
				UNION ALL
					SELECT
						n.pipeline_code,
						n.goods_spu,
						n.lang,
						0 AS rank
					FROM
						dw_gearbest_recommend.goods_backup_mid n
					JOIN dw_gearbest_recommend.gb_paltform_hotsell_top100 m ON n.pipeline_code = m.pipeline_code
					AND n.goods_spu = m.goods_spu
					GROUP BY
						n.pipeline_code,
						n.goods_spu,
						n.lang
			) t
	) x
WHERE
	x.flag <= 50
GROUP BY
	x.pipeline_code,
	x.goods_spu,
	x.lang
;



CREATE TABLE IF NOT EXISTS `dw_gearbest_recommend.goods_static_backup_result`(
  `good_sn` string, 
  `webgoodsn` string, 
  `pipeline_code` string, 
  `goodstitle` string, 
  `lang` string, 
  `categoryid` int, 
  `catid` int, 
  `warecode` int, 
  `reviewcount` bigint, 
  `avgrate` double, 
  `shopprice` double, 
  `appdisplayprice` double, 
  `displayprice` double, 
  `discount` string, 
  `favoritecount` int, 
  `goodsnum` int, 
  `imgurl` string, 
  `gridurl` string, 
  `thumburl` string, 
  `thumbextendurl` string, 
  `originalUrl` string, 
  `url_title` string
    )
;




--最终结果汇总,准备写入Redis的数据结果
INSERT overwrite TABLE dw_gearbest_recommend.goods_static_backup_result SELECT
	t1.good_sn,
	t3.goods_web_sku,
	t1.pipeline_code,
	t3.good_title,
	t1.lang,
	t3.id as categoryid,
        t3.id as catid,
	t3.v_wh_code,
	t3.total_num,
	t3.avg_score,
	t3.shop_price,
	t3.shop_price,
	t3.shop_price,
	"",
	t3.total_favorite,
	t3.stock_qty,
	t3.img_url,
	t3.grid_url,
	t3.thumb_url,
	t3.thumb_extend_url,
	t2.original_url,
	t3.url_title
FROM
	(
		SELECT
			m.pipeline_code,
			m.goods_spu,
			m.lang,
			n.good_sn
		FROM
			dw_gearbest_recommend.goods_static_backup_mid m
		JOIN dw_gearbest_recommend.goods_info_mid5 n ON m.goods_spu = n.goods_spu
	) t1
JOIN(
	SELECT
		good_sn,
		original_url
	FROM
		dw_gearbest_recommend.goods_info_mid2
	GROUP BY
		good_sn,
		original_url
	)t2
ON
	t1.good_sn = t2.good_sn
JOIN
( 
	SELECT n.* FROM
	dw_gearbest_recommend.goods_info_result_uniqlang n 
WHERE
	n.good_sn NOT IN (
		SELECT
			x.good_sn
		FROM
			(
				SELECT
					a.good_sn
				FROM
					ods.ods_m_gearbest_base_goods_goods a
				WHERE
					a.recommended_level = 14
			) x
	)
) t3
ON t1.good_sn = t3.good_sn
AND t1.pipeline_code = t3.pipeline_code
AND t1.lang = t3.lang
;              