--@author ZhanRui
--@date 2018年11月29日 
--@desc  gb黑灰标打标

SET mapred.job.name=gb_black_gray_result;
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


--商品曝光数
INSERT OVERWRITE TABLE dw_gearbest_recommend.gb_7days_exp
SELECT
  m.sku,
  count(*) AS exp_num
FROM
  (
    SELECT
      log_id,
      get_json_object(glb_ubcta_json, '$.sku') as sku
    FROM
      stg.stg_gb_pc_event_info_ubcta
    WHERE
      concat_ws('-', year, month, day) BETWEEN '${ADD_TIME_W}'
      AND '${ADD_TIME}'
      AND get_json_object(glb_ubcta_json, '$.sku') <> ''
  ) m
  INNER JOIN (
    SELECT
      log_id
    FROM
      stg.gb_pc_event_info
    WHERE
      concat_ws('-', year, month, day) BETWEEN '${ADD_TIME_W}'
      AND '${ADD_TIME}'
      AND glb_t = 'ie'
      AND glb_ubcta <> ''
  ) n ON m.log_id = n.log_id
group by
  m.sku;






--商品点击数
INSERT OVERWRITE TABLE dw_gearbest_recommend.gb_7days_cli
SELECT
  get_json_object(glb_skuinfo, '$.sku') as sku,
  count(*) AS click_num
FROM
  stg.gb_pc_event_info
WHERE
  concat_ws('-', year, month, day) BETWEEN '${ADD_TIME_W}'
  AND '${ADD_TIME}'
  AND glb_t = 'ic'
  AND glb_skuinfo <> ''
group by
  get_json_object(glb_skuinfo, '$.sku') ;





--黑灰标打标
INSERT OVERWRITE TABLE dw_gearbest_recommend.gb_black_gray_tmp
SELECT
	x.sku,
    x.exp_num,
    x.click_num,
	CASE
WHEN (x.ctr < 0.003) THEN
	'black'
WHEN (0.003 <= x.ctr AND x.ctr < 0.006) THEN
	'gray'
WHEN (x.ctr >= 0.006) THEN
	'normal'
    ELSE 'black'
END AS flag
FROM
	(
		SELECT
			m.sku,
            m.exp_num,
            n.click_num,
			n.click_num / m.exp_num AS ctr
		FROM
			dw_gearbest_recommend.gb_7days_exp m
		LEFT JOIN dw_gearbest_recommend.gb_7days_cli n ON m.sku = n.sku
	) x
;

--最终结果
INSERT OVERWRITE TABLE dw_gearbest_recommend.gb_black_gray_result PARTITION (add_time = '${ADD_TIME}')
SELECT
	p.sku,
	p.type,
	p.flag
FROM
	(
		SELECT
			x.sku,
			x.type,
			CASE
		WHEN (
			x.type = '3A'
			AND x.exp_num >= 1000
		) THEN
			x.flag
		WHEN (
			x.type = '2A'
			AND x.exp_num >= 300
		) THEN
			x.flag
		WHEN (
			x.type = 'normal'
			AND x.exp_num >= 100
		) THEN
			x.flag
		END AS flag
		FROM
			(
				SELECT
					sku,
					CASE
				WHEN (n.goods_sn IS NOT NULL) THEN
					'3A'
				WHEN (t.goods_sn IS NOT NULL) THEN
					'2A'
				ELSE
					'normal'
				END AS type,
				m.exp_num,
				m.flag
			FROM
				dw_gearbest_recommend.gb_black_gray_tmp m
			LEFT JOIN dw_gearbest_recommend.gb_3a_goods n ON m.sku = n.goods_sn
			LEFT JOIN dw_gearbest_recommend.gb_2a_goods t ON m.sku = t.goods_sn
			) x
	) p
WHERE
	p.flag IS NOT NULL
GROUP BY 
	p.sku,
	p.type,
	p.flag
;

--bf商品黑灰标结果
INSERT OVERWRITE TABLE dw_gearbest_recommend.gb_bf_black_gray_result 
SELECT
	m.sku,
	m.type,
	m.flag
FROM
	(
		SELECT
			p.sku,
			p.type,
			p.flag
		FROM
			dw_gearbest_recommend.gb_black_gray_result p
		WHERE
			p.add_time = '${ADD_TIME}'
	) m
JOIN (
	SELECT
		a.good_sn
	FROM
		ods.ods_m_gearbest_base_goods_goods a
	WHERE
		a.dt = '${YEAR}${MONTH}${DAY}'
	AND a.recommended_level = 14
) n ON m.sku = n.good_sn