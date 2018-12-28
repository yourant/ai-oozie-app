
--@author ZhanRui
--@date 2018年6月25日 
--@desc  Zaful分类列表页报表

SET mapred.job.name=zaful_cat_report;
set mapred.job.queue.name=root.ai.offline;
SET mapred.max.split.size=128000000;
SET mapred.min.split.size=128000000;
SET mapred.min.split.size.per.node=128000000;
SET mapred.min.split.size.per.rack=128000000;
SET hive.exec.reducers.bytes.per.reducer = 128000000;
SET hive.merge.mapfiles=true;
SET hive.merge.mapredfiles= true;
SET hive.input.format=org.apache.hadoop.hive.ql.io.CombineHiveInputFormat;
SET hive.merge.size.per.task=256000000;
SET hive.exec.parallel = true; 



--UV需要单独计算，分类合并的时候需要对cookie去重，不能直接将不同的分类UV相加
INSERT overwrite TABLE  dw_proj.zaful_cat_uv PARTITION (add_time = '${ADD_TIME}')
SELECT
	count(*) AS uv,
	m.glb_plf,
	m.glb_p,
  m.glb_dc,
	m.geoip_country_name
FROM
	(
		SELECT
			x.glb_od,
			x.glb_plf,
			p.cat_id AS glb_p,
			x.geoip_country_name,
			x.glb_dc
		FROM
			(
				SELECT
					glb_od,
					glb_plf,
					split (glb_p, '-') [ 0 ] AS glb_p,
					geoip_country_name,
					glb_dc
				FROM
					stg.zf_pc_event_info
				WHERE
				concat_ws('-', YEAR, MONTH, DAY) = '${ADD_TIME}'
				AND glb_t = 'ie'
				AND glb_s = 'b01'
				AND glb_ubcta = ''
				GROUP BY
					glb_od,
					glb_plf,
					split (glb_p, '-') [ 0 ], 
          geoip_country_name,
					glb_dc
			) x
		JOIN dw_proj.cat_name_child p ON x.glb_p = p.child_id
		GROUP BY
			x.glb_od,
			x.glb_plf,
			p.cat_id,
			x.geoip_country_name,
			x.glb_dc
	) m
GROUP BY
	m.glb_plf,
	m.glb_p,
	m.geoip_country_name,
	m.glb_dc;
;


--PV
INSERT OVERWRITE TABLE  dw_proj.cat_pv_tmp
SELECT
  count(*) as pv,
  glb_plf,
  split(glb_p,'-')[0] as glb_p,
  geoip_country_name,
  glb_dc,
  '${ADD_TIME}' as add_time
FROM
  stg.zf_pc_event_info
WHERE
  concat_ws('-', year, month, day) = '${ADD_TIME}'
  and glb_t = 'ie'
  and glb_s = 'b01'
  and glb_ubcta = ''
group by
  glb_plf,
  split(glb_p,'-')[0],
  geoip_country_name,
  glb_dc;


--页面UV
INSERT OVERWRITE TABLE dw_proj.cat_uv_tmp
SELECT
  count(*) AS uv,
  m.glb_plf,
  m.glb_p,
  m.geoip_country_name,
  m.glb_dc,
  '${ADD_TIME}' as add_time
FROM
  (
    SELECT
      glb_od,
      glb_plf,
      split(glb_p,'-')[0] as glb_p,
      geoip_country_name,
      glb_dc
    FROM
      stg.zf_pc_event_info
    WHERE
      concat_ws('-', year, month, day) = '${ADD_TIME}'
      and glb_t = 'ie'
      and glb_s = 'b01'
      and glb_ubcta = ''
    GROUP BY
      glb_od,
      glb_plf,
      split(glb_p,'-')[0],
      geoip_country_name,
      glb_dc
  ) m
group by
  m.glb_plf,
  m.glb_p,
  m.geoip_country_name,
  m.glb_dc;

--商品曝光数
INSERT OVERWRITE TABLE dw_proj.cat_exp_tmp
SELECT
  count(*) AS exp_num,
  n.glb_plf,
  n.glb_p,
  n.geoip_country_name,
  n.glb_dc,
  '${ADD_TIME}' as add_time
FROM
  (
    SELECT
      log_id,
      get_json_object(glb_ubcta_col, '$.sku') as sku
    FROM
      stg.zf_pc_event_ubcta_info
    WHERE
      concat_ws('-', year, month, day) = '${ADD_TIME}'
      AND get_json_object(glb_ubcta_col, '$.sku') <> ''
  ) m
  INNER JOIN (
    SELECT
      log_id,
      glb_plf,
      split(glb_p,'-')[0] as glb_p,
      geoip_country_name,
      glb_dc
    FROM
      stg.zf_pc_event_info
    WHERE
      concat_ws('-', year, month, day) = '${ADD_TIME}'
      and glb_t = 'ie'
      and glb_s = 'b01'
      and glb_pm = 'mp'
      AND glb_ubcta <> ''
  ) n ON m.log_id = n.log_id
group by
  n.glb_plf,
  n.glb_p,
  n.geoip_country_name,
  n.glb_dc;

--商品点击数
INSERT OVERWRITE TABLE dw_proj.cat_click_tmp
SELECT
  count(*) AS click_num,
  glb_plf,
  split(glb_p,'-')[0] as glb_p,
  geoip_country_name,
  glb_dc,
  '${ADD_TIME}' as add_time
FROM
  stg.zf_pc_event_info
WHERE
  concat_ws('-', year, month, day) = '${ADD_TIME}'
  AND glb_t = 'ic'
  and glb_s = 'b01'
  and glb_pm = 'mp' 
  AND glb_x in ('sku','addtobag')
group by
  glb_plf,
  split(glb_p,'-')[0],
  geoip_country_name,
  glb_dc;



--商品加购数
INSERT OVERWRITE TABLE dw_proj.cat_cart_tmp
SELECT SUM(m.pam) as cart_num,
m.glb_plf,
n.cat_id,
m.geoip_country_name,
m.glb_dc,
'${ADD_TIME}' as add_time
FROM
(
    SELECT
        get_json_object(glb_skuinfo, '$.pam') as pam,
        glb_plf,
        get_json_object(glb_skuinfo, '$.sku') as sku,
        geoip_country_name,
        glb_dc
    FROM
        stg.zf_pc_event_info
    WHERE
        concat_ws('-', year, month, day) = '${ADD_TIME}'
        AND glb_t = 'ic'
        AND glb_x = 'ADT'
        and get_json_object(glb_ubcta, '$.fmd')='mp'
        and get_json_object(glb_ubcta, '$.sckw') is null
) m 
JOIN stg.zaful_eload_goods n ON m.sku=n.goods_sn 
group by
  m.glb_plf,
  n.cat_id,
  m.geoip_country_name,
  m.glb_dc
;





--中间表report_sku_user_tmp：取sku,user_id
INSERT OVERWRITE TABLE dw_proj.cat_sku_user_country_tmp
SELECT
  m.sku,
  n.glb_u,
  m.glb_plf,
  m.geoip_country_name,
  m.glb_dc
FROM
  (
    SELECT
      glb_od,
      regexp_extract(glb_skuinfo, '(.*?sku":")([0-9a-zA-Z]*)(".*?)', 2) AS sku,
      glb_plf,
      geoip_country_name,
      glb_dc
    FROM
      stg.zf_pc_event_info
    WHERE
      concat_ws('-', year, month, day) BETWEEN '${ADD_TIME_W}'
      AND '${ADD_TIME}'
      AND glb_t = 'ic'
      AND glb_x = 'ADT'
      and get_json_object(glb_ubcta, '$.fmd')='mp'
      and get_json_object(glb_ubcta, '$.sckw') is null
      AND glb_skuinfo <> ''
      AND glb_ubcta <> ''
  ) m
  INNER JOIN dw_zaful_recommend.zaful_od_u_map n ON m.glb_od = n.glb_od
GROUP BY
  m.glb_plf,
  m.sku,
  n.glb_u,
  m.geoip_country_name,
  m.glb_dc;



--中间表report_sku_user_tmp：取sku,user_id
INSERT OVERWRITE TABLE dw_proj.cat_sku_user_tmp
SELECT
  p.goods_sn,
  x.user_id,
  x.order_status,
  p.pay_amount,
  q.cat_id,
  p.goods_number
FROM
  (
    SELECT
      order_id,
      user_id,
      add_time,
      order_status
    FROM
      stg.zaful_eload_order_info
    WHERE
      from_unixtime(add_time, 'yyyy-MM-dd') = '${ADD_TIME}'
  ) x
  JOIN  (
  SELECT goods_sn,order_id,
  goods_number,
  case when goods_pay_amount <> '0' then goods_pay_amount
  else goods_price*goods_number end as pay_amount
  from
   stg.zaful_eload_order_goods ) p ON x.order_id = p.order_id
  JOIN stg.zaful_eload_goods q ON p.goods_sn = q.goods_sn
group by
  p.goods_sn,
  x.user_id,
  x.order_status,
  p.pay_amount,
  q.cat_id,
  p.goods_number
;


--商品下单数
INSERT OVERWRITE TABLE dw_proj.cat_order_tmp
SELECT
  SUM(x2.goods_number) AS order_num,
  x1.glb_plf,
  x2.cat_id,
  x1.country,
  x1.glb_dc,
  '${ADD_TIME}' as add_time
from
  dw_proj.cat_sku_user_country_tmp x1
  INNER JOIN dw_proj.cat_sku_user_tmp x2 ON x1.user_id = x2.user_id
  AND x1.goods_sn = x2.goods_sn
group by
  x1.glb_plf,
  x2.cat_id,
  x1.country,
  x1.glb_dc;


--支付订单数（销量）
INSERT OVERWRITE TABLE dw_proj.cat_purchase_tmp
SELECT
  SUM(x2.goods_number) AS purchase_num,
  x1.glb_plf,
  x2.cat_id,
  x1.country,
  x1.glb_dc,
  '${ADD_TIME}' as add_time
from
  dw_proj.cat_sku_user_country_tmp x1
  INNER JOIN dw_proj.cat_sku_user_tmp x2 ON x1.user_id = x2.user_id
  AND x1.goods_sn = x2.goods_sn
where
  x2.order_status not in ('0', '11')
group by
  x1.glb_plf,
  x2.cat_id,
  x1.country,
  x1.glb_dc;


--购买金额（销售额）
INSERT OVERWRITE TABLE dw_proj.cat_pay_amount_tmp
SELECT
  SUM(x2.pay_amount) AS pay_amount,
  x1.glb_plf,
  x2.cat_id,
  x1.country,
  x1.glb_dc,
  '${ADD_TIME}' as add_time
from
  dw_proj.cat_sku_user_country_tmp x1
  INNER JOIN dw_proj.cat_sku_user_tmp x2 ON x1.user_id = x2.user_id
  AND x1.goods_sn = x2.goods_sn
where
  x2.order_status not in ('0', '11')
group by
  x1.glb_plf,
  x2.cat_id,
  x1.country,
  x1.glb_dc;

--商品收藏数
INSERT OVERWRITE TABLE dw_proj.cat_collect_tmp
SELECT count(*) as collect_num,
m.glb_plf,
n.cat_id,
m.geoip_country_name,
m.glb_dc,
'${ADD_TIME}' as add_time
FROM
(
    SELECT
        glb_plf,
        get_json_object(glb_skuinfo, '$.sku') as sku,
        geoip_country_name,
        glb_dc
    FROM
        stg.zf_pc_event_info
    WHERE
        concat_ws('-', year, month, day) = '${ADD_TIME}'
        AND glb_t = 'ic'
        AND glb_x = 'ADF'
        AND glb_u <> ''
        and get_json_object(glb_ubcta, '$.fmd')='mp'
        and get_json_object(glb_ubcta, '$.sckw') is null
) m 
JOIN stg.zaful_eload_goods n ON m.sku=n.goods_sn 
group by
  m.glb_plf,
  n.cat_id,
  m.geoip_country_name,
  m.glb_dc
;


--所有结果汇总
INSERT overwrite TABLE dw_proj.zaful_cat_report PARTITION (add_time = '${ADD_TIME}')
select
  a.pv,
  b.uv,
  c.exp_num,
  d.click_num,
  d.click_num / c.exp_num,
  e.cart_num,
  e.cart_num / d.click_num,
  f.order_num,
  f.order_num / e.cart_num,
  g.purchase_num,
  h.pay_amount,
  g.purchase_num / f.order_num,
  g.purchase_num / c.exp_num,
  i.collect_num,
  'PC',
  a.glb_p,
  a.glb_dc,
  a.country
from
  (select pv, add_time, glb_dc, country, glb_p from dw_proj.cat_pv_tmp where glb_plf = 'pc') a
left join 
  (select uv, add_time, glb_dc, country, glb_p from dw_proj.cat_uv_tmp where glb_plf = 'pc') b 
ON a.add_time=b.add_time and a.glb_dc=b.glb_dc and a.country=b.country and a.glb_p=b.glb_p
left join 
  (select exp_num, add_time, glb_dc, country, glb_p from dw_proj.cat_exp_tmp where glb_plf = 'pc') c 
ON a.add_time=c.add_time and a.glb_dc=c.glb_dc and a.country=c.country and a.glb_p=c.glb_p
left join 
  (select click_num, add_time, glb_dc, country, glb_p from dw_proj.cat_click_tmp where glb_plf = 'pc') d 
ON a.add_time=d.add_time and a.glb_dc=d.glb_dc and a.country=d.country and a.glb_p=d.glb_p
left join 
  (select cart_num, add_time, glb_dc, country, glb_p from dw_proj.cat_cart_tmp where glb_plf = 'pc') e 
ON a.add_time=e.add_time and a.glb_dc=e.glb_dc and a.country=e.country and a.glb_p=e.glb_p
left join 
  (select order_num, add_time, glb_dc, country, glb_p from dw_proj.cat_order_tmp where glb_plf = 'pc') f 
ON a.add_time=f.add_time and a.glb_dc=f.glb_dc and a.country=f.country and a.glb_p=f.glb_p
left join 
  (select purchase_num, add_time, glb_dc, country, glb_p from dw_proj.cat_purchase_tmp where glb_plf = 'pc') g 
ON a.add_time=g.add_time and a.glb_dc=g.glb_dc and a.country=g.country and a.glb_p=g.glb_p
left join 
  (select pay_amount, add_time, glb_dc, country, glb_p from dw_proj.cat_pay_amount_tmp where glb_plf = 'pc') h 
ON a.add_time=h.add_time and a.glb_dc=h.glb_dc and a.country=h.country and a.glb_p=h.glb_p
left join 
  (select collect_num, add_time, glb_dc, country, glb_p from dw_proj.cat_collect_tmp where glb_plf = 'pc') i
ON a.add_time=i.add_time and a.glb_dc=i.glb_dc and a.country=i.country and a.glb_p=i.glb_p

union all

select
  a.pv,
  b.uv,
  c.exp_num,
  d.click_num,
  d.click_num / c.exp_num,
  e.cart_num,
  e.cart_num / d.click_num,
  f.order_num,
  f.order_num / e.cart_num,
  g.purchase_num,
  h.pay_amount,
  g.purchase_num / f.order_num,
  g.purchase_num / c.exp_num,
  i.collect_num,
  'M',
  a.glb_p,
  a.glb_dc,
  a.country
from
  (select pv, add_time, glb_dc, country, glb_p from dw_proj.cat_pv_tmp where glb_plf = 'm') a
left join 
  (select uv, add_time, glb_dc, country, glb_p from dw_proj.cat_uv_tmp where glb_plf = 'm') b 
ON a.add_time=b.add_time and a.glb_dc=b.glb_dc and a.country=b.country and a.glb_p=b.glb_p
left join 
  (select exp_num, add_time, glb_dc, country, glb_p from dw_proj.cat_exp_tmp where glb_plf = 'm') c 
ON a.add_time=c.add_time and a.glb_dc=c.glb_dc and a.country=c.country and a.glb_p=c.glb_p
left join 
  (select click_num, add_time, glb_dc, country, glb_p from dw_proj.cat_click_tmp where glb_plf = 'm') d 
ON a.add_time=d.add_time and a.glb_dc=d.glb_dc and a.country=d.country and a.glb_p=d.glb_p
left join 
  (select cart_num, add_time, glb_dc, country, glb_p from dw_proj.cat_cart_tmp where glb_plf = 'm') e 
ON a.add_time=e.add_time and a.glb_dc=e.glb_dc and a.country=e.country and a.glb_p=e.glb_p
left join 
  (select order_num, add_time, glb_dc, country, glb_p from dw_proj.cat_order_tmp where glb_plf = 'm') f 
ON a.add_time=f.add_time and a.glb_dc=f.glb_dc and a.country=f.country and a.glb_p=f.glb_p
left join 
  (select purchase_num, add_time, glb_dc, country, glb_p from dw_proj.cat_purchase_tmp where glb_plf = 'm') g 
ON a.add_time=g.add_time and a.glb_dc=g.glb_dc and a.country=g.country and a.glb_p=g.glb_p
left join 
  (select pay_amount, add_time, glb_dc, country, glb_p from dw_proj.cat_pay_amount_tmp where glb_plf = 'm') h 
ON a.add_time=h.add_time and a.glb_dc=h.glb_dc and a.country=h.country and a.glb_p=h.glb_p
left join 
  (select collect_num, add_time, glb_dc, country, glb_p from dw_proj.cat_collect_tmp where glb_plf = 'm') i
ON a.add_time=i.add_time and a.glb_dc=i.glb_dc and a.country=i.country and a.glb_p=i.glb_p
;


INSERT overwrite table dw_proj.zaful_cat_report_exp
select
  *
FROM
  dw_proj.zaful_cat_report
where
  add_time = '${ADD_TIME}';

INSERT overwrite table dw_proj.zaful_cat_uv_exp
select
  *
FROM
  dw_proj.zaful_cat_uv
where
  add_time = '${ADD_TIME}';