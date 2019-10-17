--@author zhangyuchao
--@date 2019年09月26日 
--@desc  gb黑灰标打标

SET mapred.job.name=gb_black_gray_result_v2;
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

--a.近7天，全站全渠道曝光次数大于等于1000次，且曝光点击率（点击/曝光）小于千分之一
--b.30天前上架的sku , 近30天曝光量大于等于500，且近30天全站全渠道已支付订单量为0
--黑标过滤：标签系统中商品标签ID为282、301、302爆款地图的商品不打黑标（每日更新）
--(a 或  b ) 且 非爆款   


--商品曝光数7days
INSERT OVERWRITE TABLE dw_gearbest_recommend.gb_7days_exp_v2_tmp
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
  
--统一数据源
INSERT OVERWRITE TABLE dw_gearbest_recommend.gb_7days_exp_v2
SELECT m.sku, m.exp_num FROM
( SELECT sku, exp_num from dw_gearbest_recommend.gb_7days_exp_v2_tmp ) m
JOIN 
( SELECT distinct sku from dw_gearbest_recommend.sku_info_data ) n
ON m.sku=n.sku;
  
--商品曝光数30days---ADD_TIME_M   30days
INSERT OVERWRITE TABLE dw_gearbest_recommend.gb_30days_exp_v2_tmp
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
      concat_ws('-', year, month, day) BETWEEN '${ADD_TIME_M}'
      AND '${ADD_TIME}'
      AND get_json_object(glb_ubcta_json, '$.sku') <> ''
  ) m
  INNER JOIN (
    SELECT
      log_id
    FROM
      stg.gb_pc_event_info
    WHERE
      concat_ws('-', year, month, day) BETWEEN '${ADD_TIME_M}'
      AND '${ADD_TIME}'
      AND glb_t = 'ie'
      AND glb_ubcta <> ''
  ) n ON m.log_id = n.log_id
group by
  m.sku;  

--统一数据源
INSERT OVERWRITE TABLE dw_gearbest_recommend.gb_30days_exp_v2
SELECT m.sku, m.exp_num FROM
( SELECT sku, exp_num from dw_gearbest_recommend.gb_30days_exp_v2_tmp ) m
JOIN 
( SELECT distinct sku from dw_gearbest_recommend.sku_info_data ) n
ON m.sku=n.sku;

--商品点击数7days
INSERT OVERWRITE TABLE dw_gearbest_recommend.gb_7days_cli_v2_tmp
SELECT 
	t.sku AS sku,
	CASE
		WHEN ( t.click_num IS NULL) THEN
			0
		ELSE t.click_num
	END AS click_num
FROM
(
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
  get_json_object(glb_skuinfo, '$.sku') 
) t
;

--统一数据源
INSERT OVERWRITE TABLE dw_gearbest_recommend.gb_7days_cli_v2
SELECT m.sku, m.click_num FROM
( SELECT sku, click_num from dw_gearbest_recommend.gb_7days_cli_v2_tmp ) m
JOIN 
( SELECT distinct sku from dw_gearbest_recommend.sku_info_data ) n
ON m.sku=n.sku;
  
--黑灰标打标1-点击/曝光 规则
-- a.近7天，全站全渠道曝光次数大于等于1000次，且曝光点击率（点击/曝光）小于千分之一
INSERT OVERWRITE TABLE dw_gearbest_recommend.gb_black_gray_tmp_v2
SELECT
	x.sku,
    x.exp_num,
    x.click_num,
	CASE
		WHEN ( x.ctr < 0.001 AND x.exp_num >= 1000) THEN
			'black'
		ELSE 'normal'
	END AS flag
FROM
	(
		SELECT
			m.sku,
			CASE
				WHEN ( m.exp_num IS NULL ) THEN
				0
			ELSE m.exp_num
			END AS exp_num,
			CASE
				WHEN ( n.click_num IS NULL ) THEN
				0
			ELSE n.click_num
			END AS click_num,
			CASE
				WHEN ( n.click_num IS NULL ) THEN
				0
			ELSE n.click_num / m.exp_num
			END AS ctr
		FROM
			dw_gearbest_recommend.gb_7days_exp_v2 m
		LEFT JOIN dw_gearbest_recommend.gb_7days_cli_v2 n ON m.sku = n.sku
	) x
;

--b近30天，全站全渠道已支付订单量 sku - qty 
--stg.gb_order_order_info
--ods.ods_m_gearbest_gb_order_order_info
--order_status	int	订单状态：0-正常,1-用户取消,2-系统取消,3-运营取消,4-Oms取消,5-取消申请中,6-完结
--pay_status	int	支付状态：0-未支付，1-部分支付，2-pending，3-已支付,4-退款中，5-已退款
--created_time	int	订单创建时间
--completed_time	int	完成支付时间
INSERT OVERWRITE TABLE dw_gearbest_recommend.gb_30days_order_num
SELECT
  a.goods_sn,
  count(a.order_sn) qty
FROM
  stg.gb_order_order_goods a
JOIN
	(
		SELECT
		  order_sn
		FROM
		  stg.gb_order_order_info
		WHERE
		  pay_status = 3
		  AND completed_time < UNIX_TIMESTAMP()
		  AND completed_time > UNIX_TIMESTAMP(DATE_SUB(FROM_UNIXTIME(UNIX_TIMESTAMP(), 'yyyy-MM-dd'), 30),'yyyy-MM-dd')
	) e
ON a.order_sn=e.order_sn group by goods_sn 
;

--c全站全渠道所有的上架sku的上架时间满30天sku  
INSERT OVERWRITE TABLE dw_gearbest_recommend.gb_uptime_30days_sku
SELECT 
	distinct m.sku AS good_sn,
	m.addtime AS first_up_time
FROM 
(
SELECT 
	sku,
	addtime,
	ROW_NUMBER() OVER(
        PARTITION BY sku
        ORDER BY
          addtime ASC
      ) timerank
FROM 
	dw_gearbest_recommend.sku_info_data WHERE addtime != 0
) m
WHERE m.timerank=1 AND datediff('${ADD_TIME}',FROM_UNIXTIME(m.addtime, 'yyyy-MM-dd')) >= 30 
;


--爆款的sku集合
INSERT OVERWRITE TABLE dw_gearbest_recommend.gb_goods_label_bao
SELECT
    distinct m.good_sn
FROM 
(SELECT id, good_sn FROM ods.ods_m_ai_glbg_db_goods WHERE dt='${DATE}') m 
JOIN 
(SELECT goods_id FROM ods.ods_m_ai_glbg_db_goods_label_bind WHERE dt='${DATE}' AND label_id IN ('282','301','302')) n 
ON 
    m.id = n.goods_id
;

--对于规则b
--30天前上架的sku , 近30天曝光量大于等于500，且近30天全站全渠道已支付订单量为0
INSERT OVERWRITE TABLE dw_gearbest_recommend.gb_black_sku
SELECT m.sku FROM (SELECT sku FROM dw_gearbest_recommend.gb_30days_exp_v2 WHERE exp_num >= 500) m 
LEFT JOIN (SELECT goods_sn FROM dw_gearbest_recommend.gb_30days_order_num WHERE qty > 0) n
ON m.sku = n.goods_sn
WHERE n.goods_sn IS NULL
;

---近30天，全站全渠道已支付订单量 ：gb_30days_order_num
---商品曝光数30days ：gb_30days_exp_v2
--select sku from (SELECT m.sku FROM dw_gearbest_recommend.gb_30days_exp_v2 m where m.exp_num >= 500 -- 黑标) where sku 
--not in ( select goods_sn from dw_gearbest_recommend.gb_30days_order_num where qty > 0 --非黑标)

--黑灰标打标2-支付订单量 规则
INSERT OVERWRITE TABLE dw_gearbest_recommend.gb_30days_order_num_black
SELECT 
	m.goods_sn,'black' as flag
FROM 
	dw_gearbest_recommend.gb_uptime_30days_sku  m
JOIN
	dw_gearbest_recommend.gb_black_sku  n
ON 
	m.goods_sn = n.sku 
;

--最终结果
INSERT OVERWRITE TABLE dw_gearbest_recommend.gb_black_gray_result_v2 PARTITION (add_time = '${ADD_TIME}')
SELECT distinct p.sku, p.flag 
FROM 
(
	SELECT sku , flag FROM dw_gearbest_recommend.gb_black_gray_tmp_v2  WHERE flag='black' 
	UNION ALL 
	SELECT good_sn as sku, flag FROM dw_gearbest_recommend.gb_30days_order_num_black WHERE flag='black' 
) p 
LEFT JOIN dw_gearbest_recommend.gb_goods_label_bao q
ON p.sku = q.goods_sn
WHERE q.goods_sn  IS NULL
;

--ods.ods_m_ai_glbg_db_goods                        glbg_db.goods
--ods.ods_m_ai_glbg_db_goods_label_bind        glbg_db.goods_label_bind

--select goods_id from ods.ods_m_ai_glbg_db_goods_label_bind where dt = '20191007' and label_id=282 
--(a 或  b ) 且 非爆款   
