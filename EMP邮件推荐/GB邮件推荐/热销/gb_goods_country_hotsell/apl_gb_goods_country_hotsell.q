--@author ZhanRui
--@date 2018年11月05日 
--@desc  gb邮件推荐按国家取热销数据

SET mapred.job.name=apl_gb_goods_country_hotsell;
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

--用户、国家对应关系
INSERT OVERWRITE TABLE dw_proj.gb_uc_map
SELECT
  m.glb_u,
  m.geoip_city_country_code,
  m.geoip_country_name
from
  (
    SELECT
      glb_u,
      geoip_city_country_code,
      geoip_country_name
    FROM
      dw_proj.gb_uc_map
    union all
    SELECT
      glb_u,
      geoip_city_country_code,
      geoip_country_name
    FROM
      stg.gb_pc_event_info
    WHERE
      concat_ws('-', year, month, day) = '${ADD_TIME}'
      AND glb_u rlike '^[0-9]+$'
      and geoip_city_country_code <> ''
  ) m
GROUP BY
  m.glb_u,
  m.geoip_city_country_code,
  m.geoip_country_name
;


--按国家取前15天spu销量,销售额
INSERT OVERWRITE TABLE dw_gearbest_recommend.goods_country_count
SELECT
	c.goods_spu,
	b.qty,
	b.pay_amount,
    d.geoip_city_country_code as country_code
FROM
	(
		SELECT
			a.goods_sn,
			a.qty,
            e.user_id,
			a.qty*a.price as pay_amount
		FROM
			stg.gb_order_order_goods a
		JOIN (
			SELECT
				order_sn,
                user_id
			FROM
				stg.gb_order_order_info
			WHERE
				order_status = 0
			AND created_time < UNIX_TIMESTAMP()
			AND created_time > UNIX_TIMESTAMP(
				DATE_SUB(
					FROM_UNIXTIME(
						UNIX_TIMESTAMP(),
						'yyyy-MM-dd'
					),
					15
				),
				'yyyy-MM-dd'
			)
		) e ON a.order_sn = e.order_sn
	) b
JOIN dw_gearbest_recommend.goods_info_mid1 c ON b.goods_sn = c.good_sn
JOIN dw_proj.gb_uc_map d ON b.user_id = d.glb_u
;

--热销top 50
INSERT OVERWRITE TABLE dw_gearbest_recommend.goods_country_hotsell
SELECT
	x.country_code,
	x.goods_spu,
	x.sell_amount,
	x.spurank
FROM
	(
		SELECT
			m.country_code,
			m.goods_spu,
			m.sell_amount,
			ROW_NUMBER () OVER (
				PARTITION BY m.country_code
			ORDER BY
				sell_amount DESC
			) AS spurank
		FROM
			(
				SELECT
					a.country_code,
					a.goods_spu,
					SUM(a.pay_amount) as sell_amount
				FROM
					dw_gearbest_recommend.goods_country_count a
				GROUP BY
					a.country_code,
					a.goods_spu
			) m
	) x
WHERE
	x.spurank <= 50;

--关联商品池，补全商品信息
INSERT OVERWRITE TABLE dw_gearbest_recommend.apl_gb_goods_country_hotsell
SELECT
    t1.country_code,
	t2.good_sn,
    t3.pipeline_code,
	t3.goods_web_sku as webGoodSn,
	t3.good_title as goodsTitle,
	t3.lang,
    t3.shop_code  as shopCode,
    t3.id as  catId,
	t3.v_wh_code as wareCode,
	t3.total_num as reviewCount,
	t3.avg_score as avgRate,
	t3.shop_price as shopPrice,
	t3.total_favorite as favoriteCount,
	t3.stock_qty as goodsNum,
	t3.img_url as imgUrl,
	t3.grid_url as gridUrl,
	t3.thumb_url as thumbUrl,
	t3.thumb_extend_url as thumbExtendUrl,
	t3.url_title
FROM(
	SELECT
		goods_spu,
		country_code
	FROM
		dw_gearbest_recommend.goods_country_hotsell
	) t1
JOIN
	dw_gearbest_recommend.goods_info_mid5 t2
ON
	t1.goods_spu = t2.goods_spu
JOIN
	dw_gearbest_recommend.goods_info_result_uniqlang t3
ON
	t2.good_sn = t3.good_sn
;