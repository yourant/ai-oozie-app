
--@author ZhanRui
--@date 2018年5月29日 
--@desc  gb推荐位报表按国家切流量统计

SET mapred.job.name=gb_recommend_position_report;
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


--cookie user 对应关系表每日更新埋点信息，汇总全量。此表先跑，其他任务可能用到。
INSERT OVERWRITE TABLE dw_gearbest_report.gb_od_u_map
SELECT
  m.glb_od,
  m.glb_u
from
  (
    SELECT
      glb_od,
      glb_u
    FROM
      dw_gearbest_report.gb_od_u_map
    union all
    SELECT
      glb_od,
      glb_u
    FROM
      stg.gb_pc_event_info
    WHERE
      concat_ws('-', year, month, day) = '${ADD_TIME}'
      AND glb_u rlike '^[0-9]+$'
  ) m
GROUP BY
  m.glb_od,
  m.glb_u
;

--中间表report_sku_user_tmp：取sku,user_id
INSERT OVERWRITE TABLE dw_gearbest_report.report_sku_user_country_tmp
SELECT
  m.sku,
  n.glb_u,
  m.glb_plf,
  m.geoip_country_name,
  m.glb_dc,
  m.fmd
FROM
  (
    SELECT
      glb_od,
      regexp_extract(glb_skuinfo, '(.*?sku":")([0-9a-zA-Z]*)(".*?)', 2) AS sku,
      glb_plf,
      geoip_country_name,
      glb_dc,
      get_json_object(glb_ubcta, '$.fmd') AS fmd
    FROM
      stg.gb_pc_event_info
    WHERE
      concat_ws('-', year, month, day) BETWEEN '${ADD_TIME_W}'
      AND '${ADD_TIME}'
      AND glb_t = 'ic'
      AND glb_x = 'ADT'
      AND glb_skuinfo <> ''
      AND glb_ubcta <> ''
  ) m
  INNER JOIN dw_gearbest_report.gb_od_u_map n ON m.glb_od = n.glb_od
GROUP BY
  m.glb_plf,
  m.fmd,
  m.sku,
  n.glb_u,
  m.geoip_country_name,
  m.glb_dc;


--中间表report_sku_user_tmp：取sku,user_id
INSERT OVERWRITE TABLE dw_gearbest_report.report_sku_user_tmp
SELECT
  p.goods_sn,
  x.user_id,
  x.order_status,
  p.price * p.qty as pay_amount
FROM
  (
    SELECT
      order_sn,
      user_id,
      created_time,
      order_status
    FROM
      stg.gb_order_order_info
    WHERE
      from_unixtime(created_time, 'yyyy-MM-dd') = '${ADD_TIME}'
  ) x
  INNER JOIN stg.gb_order_order_goods p ON x.order_sn = p.order_sn;


--页面PV
INSERT OVERWRITE TABLE dw_gearbest_report.report_pv_tmp
SELECT
  count(*) as pv,
  glb_plf,
  glb_b,
  geoip_country_name,
  glb_dc,
  '${ADD_TIME}' as add_time
FROM
  stg.gb_pc_event_info
WHERE
  year = '${YEAR}'
  AND month = '${MONTH}'
  AND day = '${DAY}'
  and glb_t = 'ie'
  and glb_ubcta = ''
group by
  glb_plf,
  glb_b,
  geoip_country_name,
  glb_dc;

--页面UV
INSERT OVERWRITE TABLE dw_gearbest_report.report_uv_tmp
SELECT
  count(*) AS uv,
  m.glb_plf,
  m.glb_b,
  m.geoip_country_name,
  m.glb_dc,
  '${ADD_TIME}' as add_time
FROM
  (
    SELECT
      glb_od,
      glb_plf,
      glb_b,
      geoip_country_name,
      glb_dc
    FROM
      stg.gb_pc_event_info
    WHERE
      year = '${YEAR}'
      AND month = '${MONTH}'
      AND day = '${DAY}'
    GROUP BY
      glb_od,
      glb_plf,
      glb_b,
      geoip_country_name,
      glb_dc
  ) m
group by
  m.glb_plf,
  m.glb_b,
  m.geoip_country_name,
  m.glb_dc;


--商品曝光数
INSERT OVERWRITE TABLE dw_gearbest_report.report_exp_tmp
SELECT
  count(*) AS exp_num,
  n.glb_plf,
  n.glb_b,
  m.mrlc,
  n.geoip_country_name,
  n.glb_dc,
  '${ADD_TIME}' as add_time
FROM
  (
    SELECT
      log_id,
      get_json_object(glb_ubcta_json, '$.mrlc') as mrlc
    FROM
      stg.stg_gb_pc_event_info_ubcta
    WHERE
      year = '${YEAR}'
      AND month = '${MONTH}'
      AND day = '${DAY}'
      AND get_json_object(glb_ubcta_json, '$.sku') <> ''
  ) m
  INNER JOIN (
    SELECT
      log_id,
      glb_plf,
      glb_b,
      geoip_country_name,
      glb_dc
    FROM
      stg.gb_pc_event_info
    WHERE
      year = '${YEAR}'
      AND month = '${MONTH}'
      AND day = '${DAY}'
      AND glb_t = 'ie'
      AND glb_pm = 'mr'
      AND glb_ubcta <> ''
  ) n ON m.log_id = n.log_id
group by
  n.glb_plf,
  n.glb_b,
  m.mrlc,
  n.geoip_country_name,
  n.glb_dc;

--商品点击数
INSERT OVERWRITE TABLE dw_gearbest_report.report_click_tmp
SELECT
  count(*) AS click_num,
  glb_plf,
  glb_b,
  get_json_object(glb_ubcta, '$.mrlc') as mrlc,
  geoip_country_name,
  glb_dc,
  '${ADD_TIME}' as add_time
FROM
  stg.gb_pc_event_info
WHERE
  year = '${YEAR}'
  AND month = '${MONTH}'
  AND day = '${DAY}'
  AND glb_t = 'ic'
  AND glb_pm = 'mr'
  AND glb_skuinfo <> ''
  AND glb_ubcta <> ''
group by
  glb_plf,
  glb_b,
  get_json_object(glb_ubcta, '$.mrlc'),
  geoip_country_name,
  glb_dc;


--商品加购数
INSERT OVERWRITE TABLE dw_gearbest_report.report_cart_tmp
SELECT
  count(*) AS cart_num,
  glb_plf,
  get_json_object(glb_ubcta, '$.fmd') as fmd,
  geoip_country_name,
  glb_dc,
  '${ADD_TIME}' as add_time
FROM
  stg.gb_pc_event_info
WHERE
  year = '${YEAR}'
  AND month = '${MONTH}'
  AND day = '${DAY}'
  AND glb_t = 'ic'
  AND glb_x = 'ADT'
group by
  glb_plf,
  get_json_object(glb_ubcta, '$.fmd'),
  geoip_country_name,
  glb_dc
;



--商品收藏数	
INSERT OVERWRITE TABLE dw_gearbest_report.report_collect_tmp
SELECT
  count(*) as collect_num,
  glb_plf,
  get_json_object(glb_ubcta, '$.fmd') AS fmd,
  geoip_country_name,
  glb_dc,
  '${ADD_TIME}' as add_time
FROM
  stg.gb_pc_event_info
WHERE
  year = '${YEAR}'
  AND month = '${MONTH}'
  AND day = '${DAY}'
  AND glb_t = 'ic'
  AND glb_x = 'ADF'
  AND glb_u <> ''
group by
  glb_plf,
  get_json_object(glb_ubcta, '$.fmd'),
  geoip_country_name,
  glb_dc;


--商品下单数
INSERT OVERWRITE TABLE dw_gearbest_report.report_order_tmp
SELECT
  count(*) AS order_num,
  x1.glb_plf,
  x1.fmd,
  x1.country,
  x1.glb_dc,
  '${ADD_TIME}' as add_time
from
  dw_gearbest_report.report_sku_user_country_tmp x1
  INNER JOIN dw_gearbest_report.report_sku_user_tmp x2 ON x1.user_id = x2.user_id
  AND x1.sku = x2.goods_sn
group by
  x1.glb_plf,
  x1.fmd,
  x1.country,
  x1.glb_dc;

--支付订单数
INSERT OVERWRITE TABLE dw_gearbest_report.report_purchase_tmp
SELECT
  count(*) AS purchase_num,
  x1.glb_plf,
  x1.fmd,
  x1.country,
  x1.glb_dc,
  '${ADD_TIME}' as add_time
from
  dw_gearbest_report.report_sku_user_country_tmp x1
  INNER JOIN dw_gearbest_report.report_sku_user_tmp x2 ON x1.user_id = x2.user_id
  AND x1.sku = x2.goods_sn
where
  x2.order_status ='0'
group by
  x1.glb_plf,
  x1.fmd,
  x1.country,
  x1.glb_dc;


--购买金额
INSERT OVERWRITE TABLE dw_gearbest_report.report_pay_amount_tmp
SELECT
  sum(x2.pay_amount) as pay_amount,
  x1.glb_plf,
  x1.fmd,
  x1.country,
  x1.glb_dc,
  '${ADD_TIME}' as add_time
from
  dw_gearbest_report.report_sku_user_country_tmp x1
  INNER JOIN dw_gearbest_report.report_sku_user_tmp x2 ON x1.user_id = x2.user_id
  AND x1.sku = x2.goods_sn
where
  x2.order_status ='0'
group by
  x1.glb_plf,
  x1.fmd,
  x1.country,
  x1.glb_dc;



--所有结果汇总
INSERT overwrite TABLE dw_gearbest_report.gb_recommend_position_report PARTITION (add_time = '${ADD_TIME}')
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
  '首页_B_1',
  '首页-Related to Items You\'ve Viewed',
  a.glb_dc,
  a.country
from
  (
    select
      pv,
      add_time,
      glb_dc,
      country
    from
      dw_gearbest_report.report_pv_tmp
    where
      glb_plf = 'pc'
      and glb_b = 'a'
  ) a
  left join (
    select
      uv,
      add_time,
      glb_dc,
      country
    from
      dw_gearbest_report.report_uv_tmp
    where
      glb_plf = 'pc'
      and glb_b = 'a'
  ) b on a.add_time = b.add_time
  and a.glb_dc = b.glb_dc
  and a.country = b.country
  left join (
    select
      exp_num,
      add_time,
      glb_dc,
      country
    from
      dw_gearbest_report.report_exp_tmp
    where
      glb_plf = 'pc'
      and glb_b = 'a'
      and mrlc = 'B_1'
  ) c on a.add_time = c.add_time
  and a.glb_dc = c.glb_dc
  and a.country = c.country
  left join (
    select
      click_num,
      add_time,
      glb_dc,
      country
    from
      dw_gearbest_report.report_click_tmp
    where
      glb_plf = 'pc'
      and glb_b = 'a'
      and mrlc = 'B_1'
  ) d on a.add_time = d.add_time
  and a.glb_dc = d.glb_dc
  and a.country = d.country
  left join (
    select
      cart_num,
      add_time,
      glb_dc,
      country
    from
      dw_gearbest_report.report_cart_tmp
    where
      glb_plf = 'pc'
      and fmd = 'mr_B_1'
  ) e on a.add_time = e.add_time
  and a.glb_dc = e.glb_dc
  and a.country = e.country
  left join (
    select
      order_num,
      add_time,
      glb_dc,
      country
    from
      dw_gearbest_report.report_order_tmp
    where
      glb_plf = 'pc'
      and fmd = 'mr_B_1'
  ) f on a.add_time = f.add_time
  and a.glb_dc = f.glb_dc
  and a.country = f.country
  left join (
    select
      purchase_num,
      add_time,
      glb_dc,
      country
    from
      dw_gearbest_report.report_purchase_tmp
    where
      glb_plf = 'pc'
      and fmd = 'mr_B_1'
  ) g on a.add_time = g.add_time
  and a.glb_dc = g.glb_dc
  and a.country = g.country
  left join (
    select
      pay_amount,
      add_time,
      glb_dc,
      country
    from
      dw_gearbest_report.report_pay_amount_tmp
    where
      glb_plf = 'pc'
      and fmd = 'mr_B_1'
  ) h on a.add_time = h.add_time
  and a.glb_dc = h.glb_dc
  and a.country = h.country
  left join (
    select
      collect_num,
      add_time,
      glb_dc,
      country
    from
      dw_gearbest_report.report_collect_tmp
    where
      glb_plf = 'pc'
      and fmd = 'mr_B_1'
  ) i on a.add_time = i.add_time
  and a.glb_dc = i.glb_dc
  and a.country = i.country
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
  'PC',
  '商详页_A_1',
  '商详页-Recommended Products for You',
  a.glb_dc,
  a.country
from
  (
    select
      pv,
      add_time,
      glb_dc,
      country
    from
      dw_gearbest_report.report_pv_tmp
    where
      glb_plf = 'pc'
      and glb_b = 'c'
  ) a
  left join (
    select
      uv,
      add_time,
      glb_dc,
      country
    from
      dw_gearbest_report.report_uv_tmp
    where
      glb_plf = 'pc'
      and glb_b = 'c'
  ) b on a.add_time = b.add_time
  and a.glb_dc = b.glb_dc
  and a.country = b.country
  left join (
    select
      exp_num,
      add_time,
      glb_dc,
      country
    from
      dw_gearbest_report.report_exp_tmp
    where
      glb_plf = 'pc'
      and glb_b = 'c'
      and mrlc = 'A_1'
  ) c on a.add_time = c.add_time
  and a.glb_dc = c.glb_dc
  and a.country = c.country
  left join (
    select
      click_num,
      add_time,
      glb_dc,
      country
    from
      dw_gearbest_report.report_click_tmp
    where
      glb_plf = 'pc'
      and glb_b = 'c'
      and mrlc = 'A_1'
  ) d on a.add_time = d.add_time
  and a.glb_dc = d.glb_dc
  and a.country = d.country
  left join (
    select
      cart_num,
      add_time,
      glb_dc,
      country
    from
      dw_gearbest_report.report_cart_tmp
    where
      glb_plf = 'pc'
      and fmd = 'mr_A_1'
  ) e on a.add_time = e.add_time
  and a.glb_dc = e.glb_dc
  and a.country = e.country
  left join (
    select
      order_num,
      add_time,
      glb_dc,
      country
    from
      dw_gearbest_report.report_order_tmp
    where
      glb_plf = 'pc'
      and fmd = 'mr_A_1'
  ) f on a.add_time = f.add_time
  and a.glb_dc = f.glb_dc
  and a.country = f.country
  left join (
    select
      purchase_num,
      add_time,
      glb_dc,
      country
    from
      dw_gearbest_report.report_purchase_tmp
    where
      glb_plf = 'pc'
      and fmd = 'mr_A_1'
  ) g on a.add_time = g.add_time
  and a.glb_dc = g.glb_dc
  and a.country = g.country
  left join (
    select
      pay_amount,
      add_time,
      glb_dc,
      country
    from
      dw_gearbest_report.report_pay_amount_tmp
    where
      glb_plf = 'pc'
      and fmd = 'mr_A_1'
  ) h on a.add_time = h.add_time
  and a.glb_dc = h.glb_dc
  and a.country = h.country
  left join (
    select
      collect_num,
      add_time,
      glb_dc,
      country
    from
      dw_gearbest_report.report_collect_tmp
    where
      glb_plf = 'pc'
      and fmd = 'mr_A_1'
  ) i on a.add_time = i.add_time
  and a.glb_dc = i.glb_dc
  and a.country = i.country
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
  'PC',
  '商详页_A_2',
  '商详页-Customers Who Bought This Item Also Bought',
  a.glb_dc,
  a.country
from
  (
    select
      pv,
      add_time,
      glb_dc,
      country
    from
      dw_gearbest_report.report_pv_tmp
    where
      glb_plf = 'pc'
      and glb_b = 'c'
  ) a
  left join (
    select
      uv,
      add_time,
      glb_dc,
      country
    from
      dw_gearbest_report.report_uv_tmp
    where
      glb_plf = 'pc'
      and glb_b = 'c'
  ) b on a.add_time = b.add_time
  and a.glb_dc = b.glb_dc
  and a.country = b.country
  left join (
    select
      exp_num,
      add_time,
      glb_dc,
      country
    from
      dw_gearbest_report.report_exp_tmp
    where
      glb_plf = 'pc'
      and glb_b = 'c'
      and mrlc = 'A_2'
  ) c on a.add_time = c.add_time
  and a.glb_dc = c.glb_dc
  and a.country = c.country
  left join (
    select
      click_num,
      add_time,
      glb_dc,
      country
    from
      dw_gearbest_report.report_click_tmp
    where
      glb_plf = 'pc'
      and glb_b = 'c'
      and mrlc = 'A_2'
  ) d on a.add_time = d.add_time
  and a.glb_dc = d.glb_dc
  and a.country = d.country
  left join (
    select
      cart_num,
      add_time,
      glb_dc,
      country
    from
      dw_gearbest_report.report_cart_tmp
    where
      glb_plf = 'pc'
      and fmd = 'mr_A_2'
  ) e on a.add_time = e.add_time
  and a.glb_dc = e.glb_dc
  and a.country = e.country
  left join (
    select
      order_num,
      add_time,
      glb_dc,
      country
    from
      dw_gearbest_report.report_order_tmp
    where
      glb_plf = 'pc'
      and fmd = 'mr_A_2'
  ) f on a.add_time = f.add_time
  and a.glb_dc = f.glb_dc
  and a.country = f.country
  left join (
    select
      purchase_num,
      add_time,
      glb_dc,
      country
    from
      dw_gearbest_report.report_purchase_tmp
    where
      glb_plf = 'pc'
      and fmd = 'mr_A_2'
  ) g on a.add_time = g.add_time
  and a.glb_dc = g.glb_dc
  and a.country = g.country
  left join (
    select
      pay_amount,
      add_time,
      glb_dc,
      country
    from
      dw_gearbest_report.report_pay_amount_tmp
    where
      glb_plf = 'pc'
      and fmd = 'mr_A_2'
  ) h on a.add_time = h.add_time
  and a.glb_dc = h.glb_dc
  and a.country = h.country
  left join (
    select
      collect_num,
      add_time,
      glb_dc,
      country
    from
      dw_gearbest_report.report_collect_tmp
    where
      glb_plf = 'pc'
      and fmd = 'mr_A_2'
  ) i on a.add_time = i.add_time
  and a.glb_dc = i.glb_dc
  and a.country = i.country
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
  'PC',
  '商详页_A_3',
  '商详页-Guess you like',
  a.glb_dc,
  a.country
from
  (
    select
      pv,
      add_time,
      glb_dc,
      country
    from
      dw_gearbest_report.report_pv_tmp
    where
      glb_plf = 'pc'
      and glb_b = 'c'
  ) a
  left join (
    select
      uv,
      add_time,
      glb_dc,
      country
    from
      dw_gearbest_report.report_uv_tmp
    where
      glb_plf = 'pc'
      and glb_b = 'c'
  ) b on a.add_time = b.add_time
  and a.glb_dc = b.glb_dc
  and a.country = b.country
  left join (
    select
      exp_num,
      add_time,
      glb_dc,
      country
    from
      dw_gearbest_report.report_exp_tmp
    where
      glb_plf = 'pc'
      and glb_b = 'c'
      and mrlc = 'A_3'
  ) c on a.add_time = c.add_time
  and a.glb_dc = c.glb_dc
  and a.country = c.country
  left join (
    select
      click_num,
      add_time,
      glb_dc,
      country
    from
      dw_gearbest_report.report_click_tmp
    where
      glb_plf = 'pc'
      and glb_b = 'c'
      and mrlc = 'A_3'
  ) d on a.add_time = d.add_time
  and a.glb_dc = d.glb_dc
  and a.country = d.country
  left join (
    select
      cart_num,
      add_time,
      glb_dc,
      country
    from
      dw_gearbest_report.report_cart_tmp
    where
      glb_plf = 'pc'
      and fmd = 'mr_A_3'
  ) e on a.add_time = e.add_time
  and a.glb_dc = e.glb_dc
  and a.country = e.country
  left join (
    select
      order_num,
      add_time,
      glb_dc,
      country
    from
      dw_gearbest_report.report_order_tmp
    where
      glb_plf = 'pc'
      and fmd = 'mr_A_3'
  ) f on a.add_time = f.add_time
  and a.glb_dc = f.glb_dc
  and a.country = f.country
  left join (
    select
      purchase_num,
      add_time,
      glb_dc,
      country
    from
      dw_gearbest_report.report_purchase_tmp
    where
      glb_plf = 'pc'
      and fmd = 'mr_A_3'
  ) g on a.add_time = g.add_time
  and a.glb_dc = g.glb_dc
  and a.country = g.country
  left join (
    select
      pay_amount,
      add_time,
      glb_dc,
      country
    from
      dw_gearbest_report.report_pay_amount_tmp
    where
      glb_plf = 'pc'
      and fmd = 'mr_A_3'
  ) h on a.add_time = h.add_time
  and a.glb_dc = h.glb_dc
  and a.country = h.country
  left join (
    select
      collect_num,
      add_time,
      glb_dc,
      country
    from
      dw_gearbest_report.report_collect_tmp
    where
      glb_plf = 'pc'
      and fmd = 'mr_A_3'
  ) i on a.add_time = i.add_time
  and a.glb_dc = i.glb_dc
  and a.country = i.country
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
  'PC',
  '商详页_A_4',
  '商详页-Sponsored Products Related to This Item',
  a.glb_dc,
  a.country
from
  (
    select
      pv,
      add_time,
      glb_dc,
      country
    from
      dw_gearbest_report.report_pv_tmp
    where
      glb_plf = 'pc'
      and glb_b = 'c'
  ) a
  left join (
    select
      uv,
      add_time,
      glb_dc,
      country
    from
      dw_gearbest_report.report_uv_tmp
    where
      glb_plf = 'pc'
      and glb_b = 'c'
  ) b on a.add_time = b.add_time
  and a.glb_dc = b.glb_dc
  and a.country = b.country
  left join (
    select
      exp_num,
      add_time,
      glb_dc,
      country
    from
      dw_gearbest_report.report_exp_tmp
    where
      glb_plf = 'pc'
      and glb_b = 'c'
      and mrlc = 'A_4'
  ) c on a.add_time = c.add_time
  and a.glb_dc = c.glb_dc
  and a.country = c.country
  left join (
    select
      click_num,
      add_time,
      glb_dc,
      country
    from
      dw_gearbest_report.report_click_tmp
    where
      glb_plf = 'pc'
      and glb_b = 'c'
      and mrlc = 'A_4'
  ) d on a.add_time = d.add_time
  and a.glb_dc = d.glb_dc
  and a.country = d.country
  left join (
    select
      cart_num,
      add_time,
      glb_dc,
      country
    from
      dw_gearbest_report.report_cart_tmp
    where
      glb_plf = 'pc'
      and fmd = 'mr_A_4'
  ) e on a.add_time = e.add_time
  and a.glb_dc = e.glb_dc
  and a.country = e.country
  left join (
    select
      order_num,
      add_time,
      glb_dc,
      country
    from
      dw_gearbest_report.report_order_tmp
    where
      glb_plf = 'pc'
      and fmd = 'mr_A_4'
  ) f on a.add_time = f.add_time
  and a.glb_dc = f.glb_dc
  and a.country = f.country
  left join (
    select
      purchase_num,
      add_time,
      glb_dc,
      country
    from
      dw_gearbest_report.report_purchase_tmp
    where
      glb_plf = 'pc'
      and fmd = 'mr_A_4'
  ) g on a.add_time = g.add_time
  and a.glb_dc = g.glb_dc
  and a.country = g.country
  left join (
    select
      pay_amount,
      add_time,
      glb_dc,
      country
    from
      dw_gearbest_report.report_pay_amount_tmp
    where
      glb_plf = 'pc'
      and fmd = 'mr_A_4'
  ) h on a.add_time = h.add_time
  and a.glb_dc = h.glb_dc
  and a.country = h.country
  left join (
    select
      collect_num,
      add_time,
      glb_dc,
      country
    from
      dw_gearbest_report.report_collect_tmp
    where
      glb_plf = 'pc'
      and fmd = 'mr_A_4'
  ) i on a.add_time = i.add_time
  and a.glb_dc = i.glb_dc
  and a.country = i.country
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
  'PC',
  '购物车页_A_1',
  '购物车页-You Might Also Like',
  a.glb_dc,
  a.country
from
  (
    select
      pv,
      add_time,
      glb_dc,
      country
    from
      dw_gearbest_report.report_pv_tmp
    where
      glb_plf = 'pc'
      and glb_b = 'd'
  ) a
  left join (
    select
      uv,
      add_time,
      glb_dc,
      country
    from
      dw_gearbest_report.report_uv_tmp
    where
      glb_plf = 'pc'
      and glb_b = 'd'
  ) b on a.add_time = b.add_time
  and a.glb_dc = b.glb_dc
  and a.country = b.country
  left join (
    select
      exp_num,
      add_time,
      glb_dc,
      country
    from
      dw_gearbest_report.report_exp_tmp
    where
      glb_plf = 'pc'
      and glb_b = 'd'
      and mrlc = 'A_1'
  ) c on a.add_time = c.add_time
  and a.glb_dc = c.glb_dc
  and a.country = c.country
  left join (
    select
      click_num,
      add_time,
      glb_dc,
      country
    from
      dw_gearbest_report.report_click_tmp
    where
      glb_plf = 'pc'
      and glb_b = 'd'
      and mrlc = 'A_1'
  ) d on a.add_time = d.add_time
  and a.glb_dc = d.glb_dc
  and a.country = d.country
  left join (
    select
      cart_num,
      add_time,
      glb_dc,
      country
    from
      dw_gearbest_report.report_cart_tmp
    where
      glb_plf = 'pc'
      and fmd = 'mr_A_1'
  ) e on a.add_time = e.add_time
  and a.glb_dc = e.glb_dc
  and a.country = e.country
  left join (
    select
      order_num,
      add_time,
      glb_dc,
      country
    from
      dw_gearbest_report.report_order_tmp
    where
      glb_plf = 'pc'
      and fmd = 'mr_A_1'
  ) f on a.add_time = f.add_time
  and a.glb_dc = f.glb_dc
  and a.country = f.country
  left join (
    select
      purchase_num,
      add_time,
      glb_dc,
      country
    from
      dw_gearbest_report.report_purchase_tmp
    where
      glb_plf = 'pc'
      and fmd = 'mr_A_1'
  ) g on a.add_time = g.add_time
  and a.glb_dc = g.glb_dc
  and a.country = g.country
  left join (
    select
      pay_amount,
      add_time,
      glb_dc,
      country
    from
      dw_gearbest_report.report_pay_amount_tmp
    where
      glb_plf = 'pc'
      and fmd = 'mr_A_1'
  ) h on a.add_time = h.add_time
  and a.glb_dc = h.glb_dc
  and a.country = h.country
  left join (
    select
      collect_num,
      add_time,
      glb_dc,
      country
    from
      dw_gearbest_report.report_collect_tmp
    where
      glb_plf = 'pc'
      and fmd = 'mr_A_1'
  ) i on a.add_time = i.add_time
  and a.glb_dc = i.glb_dc
  and a.country = i.country
;

INSERT overwrite table dw_gearbest_report.gb_recommend_position_exp
select
  *
FROM
  dw_gearbest_report.gb_recommend_position_report
where
  add_time = '${ADD_TIME}';