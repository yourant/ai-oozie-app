--@author ZhanRui
--@date 2019年03月19日 
--@desc  gb邮件推荐按国家，平台取热销数据

SET mapred.job.name=apl_gb_goods_country_plat_hotsell;
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


--按国家取30天spu销量,销售额
INSERT OVERWRITE TABLE dw_gearbest_recommend.goods_country_hot_count SELECT
	c.goods_spu,
	b.qty,
	b.pay_amount,
	b.country,
	b.platform
FROM
	(
		SELECT
			a.goods_sn,
			a.qty,
			e.user_id,
			x.country,
			x.platform,
			a.qty * a.price AS pay_amount
		FROM
			(
				SELECT
					*
				FROM
					ods.ods_m_gearbest_gb_order_order_goods
				WHERE
					dt = '${YEAR}${MONTH}${DAY}'
			) a
		JOIN (
			SELECT
				order_sn,
				user_id
			FROM
				ods.ods_m_gearbest_gb_order_order_info
			WHERE
				dt = '${YEAR}${MONTH}${DAY}'
			AND order_status = 0
			AND created_time < UNIX_TIMESTAMP()
			AND created_time > UNIX_TIMESTAMP(
				DATE_SUB(
					FROM_UNIXTIME(
						UNIX_TIMESTAMP(),
						'yyyy-MM-dd'
					),
					30
				),
				'yyyy-MM-dd'
			)
		) e ON a.order_sn = e.order_sn
		JOIN (
			SELECT
				m.user_id,
				n.country,
				n.platform
			FROM
				(
					SELECT
						user_id,
						email
					FROM
						ods.ods_m_gearbest_gb_member_user_base
					WHERE
						dt = '${YEAR}${MONTH}${DAY}'
				) m
			JOIN (
				SELECT
					country,
					platform,
					email
				FROM
					ods.ods_m_gearbest_gb_member_mem_reg_log
				WHERE
					dt = '${YEAR}${MONTH}${DAY}'
			) n ON m.email = n.email
			GROUP BY
				m.user_id,
				n.country,
				n.platform
		) x ON e.user_id = x.user_id
	) b
JOIN dw_gearbest_recommend.goods_info_mid1 c ON b.goods_sn = c.good_sn;

--热销top 30
INSERT OVERWRITE TABLE dw_gearbest_recommend.goods_country_plat_hotsell
SELECT
	upper(x.country_code) as country_code,
    x.platform,
	x.goods_spu,
	x.pay_amount,
	x.score
FROM
	(
		SELECT
			m.country_code,
            m.platform,
			m.goods_spu,
			m.pay_amount,
			ROW_NUMBER () OVER (
				PARTITION BY m.country_code,m.platform
			ORDER BY
				pay_amount DESC
			) AS score
		FROM
			(
				SELECT
					a.country as country_code,
                    a.platform,
					a.goods_spu,
					SUM(a.pay_amount) as pay_amount
				FROM
					dw_gearbest_recommend.goods_country_hot_count a
                WHERE a.country <> ''
				GROUP BY
					a.country,
                    a.platform,
					a.goods_spu
			) m
	) x
WHERE
	x.score <= 30;

--关联商品池，补全商品信息
INSERT OVERWRITE TABLE dw_gearbest_recommend.apl_gb_goods_country_plat_hotsell SELECT
	t1.country_code,
	t1.platform,
	t2.good_sn,
	t3.pipeline_code,
	t3.goods_web_sku AS webGoodSn,
	t3.good_title AS goodsTitle,
	t3.lang,
	t3.shop_code AS shopCode,
	t3.id AS catId,
	t3.v_wh_code AS wareCode,
	t3.total_num AS reviewCount,
	t3.avg_score AS avgRate,
	t3.shop_price AS shopPrice,
	t3.total_favorite AS favoriteCount,
	t3.stock_qty AS goodsNum,
	t3.img_url AS imgUrl,
	t3.grid_url AS gridUrl,
	t3.thumb_url AS thumbUrl,
	t3.thumb_extend_url AS thumbExtendUrl,
	t3.url_title,
	t1.score
FROM
	(
		SELECT
			goods_spu,
			country_code,
			platform,
			score
		FROM
			dw_gearbest_recommend.goods_country_plat_hotsell
	) t1
JOIN dw_gearbest_recommend.goods_info_mid5 t2 ON t1.goods_spu = t2.goods_spu
JOIN dw_gearbest_recommend.goods_info_result_uniqlang t3 ON t2.good_sn = t3.good_sn
;