
--@author ZhanRui
--@date 2018年6月25日 
--@desc  Zaful分类列表页报表

SET mapred.job.name=zaful_cat_report_AB;
set mapred.job.queue.name=root.ai.offline;
SET mapred.max.split.size=128000000;
SET mapred.min.split.size=32000000;
SET mapred.min.split.size.per.node=32000000;
SET mapred.min.split.size.per.rack=32000000;
SET hive.exec.reducers.bytes.per.reducer = 128000000;
SET hive.merge.mapfiles=true;
SET hive.merge.mapredfiles= true;
SET hive.input.format=org.apache.hadoop.hive.ql.io.CombineHiveInputFormat;
SET hive.merge.size.per.task=256000000;
SET hive.exec.parallel = true; 


--PV
INSERT OVERWRITE TABLE  tmp.cat_pv_tmp
SELECT
  count(*) as pv,
  glb_plf,
  get_json_object(user_bh_order_seq,'$.policy') as policy,
  get_json_object(glb_filter,'$.sort') as sort,
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
  glb_dc,
  get_json_object(user_bh_order_seq,'$.policy'),
  get_json_object(glb_filter,'$.sort') 
  ;

--UV
INSERT OVERWRITE TABLE  tmp.cat_uv_tmp
SELECT
  count(*) AS uv,
  m.glb_plf,
  m.policy,
  m.sort,
  m.glb_dc,
  '${ADD_TIME}' as add_time
FROM
  (
    SELECT
      glb_od,
      glb_plf,
      get_json_object(user_bh_order_seq,'$.policy') as policy,
      get_json_object(glb_filter,'$.sort') as sort,
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
      get_json_object(user_bh_order_seq,'$.policy'),
      get_json_object(glb_filter,'$.sort'),
      glb_dc
  ) m
group by
  m.glb_plf,
  m.policy,
  m.sort,
  glb_dc
;

--曝光
INSERT OVERWRITE TABLE  tmp.cat_exp_tmp
SELECT
   count(*) AS exp_num,
   n.glb_plf,
   n.policy,
   n.sort,
   n.glb_dc,
   n.add_time
FROM
  (
    SELECT
      log_id,
      get_json_object(glb_ubcta_col, '$.sku') as sku,
      concat_ws('-', year, month, day) as add_time
    FROM
      stg.zf_pc_event_ubcta_info
    WHERE
      concat_ws('-', year, month, day)  BETWEEN '${ADD_TIME}'
      AND '${ADD_TIME}'
      AND get_json_object(glb_ubcta_col, '$.sku') <> ''
  ) m
  INNER JOIN (
    SELECT
      log_id,
      glb_plf,
      get_json_object(user_bh_order_seq,'$.policy') as policy,
      get_json_object(glb_filter,'$.sort') as sort,
      glb_dc,
      concat_ws('-', year, month, day) as add_time
    FROM
      stg.zf_pc_event_info
    WHERE
      concat_ws('-', year, month, day)  BETWEEN '${ADD_TIME}'
      AND '${ADD_TIME}'
      and glb_t = 'ie'
      and glb_s = 'b01'
      and glb_pm = 'mp'
      AND glb_ubcta <> ''
  ) n ON m.log_id = n.log_id and m.add_time = n.add_time
group by
  n.glb_plf,
  n.policy,
  n.sort,
  n.glb_dc,
  n.add_time
  ;

--点击
INSERT OVERWRITE TABLE  tmp.cat_click_tmp
SELECT
  count(*) AS click_num,
  glb_plf,
  get_json_object(user_bh_order_seq,'$.policy') as policy,
   get_json_object(glb_filter,'$.sort') as sort,
   glb_dc,
  concat_ws('-', year, month, day) as add_time
FROM
  stg.zf_pc_event_info
WHERE
  concat_ws('-', year, month, day) BETWEEN '${ADD_TIME}'
  AND '${ADD_TIME}'
  AND glb_t = 'ic'
  and glb_s = 'b01'
  and glb_pm = 'mp' 
  AND glb_x in ('sku','addtobag')
group by
  glb_plf,
  get_json_object(user_bh_order_seq,'$.policy'),
   get_json_object(glb_filter,'$.sort'),
   glb_dc,
  concat_ws('-', year, month, day) 
;


--加购
INSERT OVERWRITE TABLE  tmp.cat_cart_tmp
SELECT SUM(m.pam) as cart_num,
m.glb_plf,
m.policy,
m.sort,
m.glb_dc,
m.add_time
FROM
(
    SELECT
        get_json_object(glb_skuinfo, '$.pam') as pam,
        glb_plf,
        get_json_object(glb_skuinfo, '$.sku') as sku,
        get_json_object(user_bh_order_seq,'$.policy') as policy,
        get_json_object(glb_ubcta,'$.sort')  as sort,
         glb_dc,
         concat_ws('-', year, month, day) as add_time
    FROM
        stg.zf_pc_event_info
    WHERE
        concat_ws('-', year, month, day)  BETWEEN '${ADD_TIME}'
        AND '${ADD_TIME}'
        AND glb_t = 'ic'
        AND glb_x = 'ADT'
        and get_json_object(glb_ubcta, '$.fmd')='mp'
        and get_json_object(glb_ubcta, '$.sckw') is null
        AND  get_json_object(glb_skuinfo, '$.pam') < 50
) m 
group by
  m.glb_plf,m.policy,m.sort,m.glb_dc,m.add_time
;


--========================================================================================
--中间表report_sku_user_tmp：取sku,user_id
INSERT OVERWRITE TABLE tmp.cat_sku_user_country_tmp
SELECT
  m.sku,
  n.glb_u,
  m.glb_plf,
  m.policy,
  m.sort,
  m.glb_dc
FROM
  (
    SELECT
      glb_od,
      regexp_extract(glb_skuinfo, '(.*?sku":")([0-9a-zA-Z]*)(".*?)', 2) AS sku,
      glb_plf,
      get_json_object(user_bh_order_seq,'$.policy') as policy,
      get_json_object(glb_ubcta,'$.sort')  as sort,
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
  m.policy,
  m.sort,
  m.glb_dc
;


INSERT OVERWRITE TABLE tmp.cat_sku_user_tmp 
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
  JOIN (
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


--下单商品数
INSERT OVERWRITE TABLE  tmp.cat_order_tmp
SELECT
  SUM(x2.goods_number) AS order_num,
  x1.glb_plf,
  x1.policy,
  x1.sort,
  x1.glb_dc,
  '${ADD_TIME}' as add_time
from
  tmp.cat_sku_user_country_tmp x1
  INNER JOIN tmp.cat_sku_user_tmp x2 ON x1.glb_u = x2.user_id
  AND x1.sku = x2.goods_sn
group by
  x1.glb_plf,
  x1.policy,
   x1.sort,
   x1.glb_dc
;

--销量
INSERT OVERWRITE TABLE  tmp.cat_purchase_tmp
SELECT
  SUM(x2.goods_number) AS purchase_num,
  x1.glb_plf,
  x1.policy,
   x1.sort,
   x1.glb_dc,
  '${ADD_TIME}' as add_time
from
  tmp.cat_sku_user_country_tmp x1
  INNER JOIN tmp.cat_sku_user_tmp x2 ON x1.glb_u = x2.user_id
  AND x1.sku = x2.goods_sn
  where
  x2.order_status not in ('0', '11')
group by
  x1.glb_plf,
  x1.policy,
   x1.sort,
   x1.glb_dc
;


--销售额
INSERT OVERWRITE TABLE  tmp.cat_pay_amount_tmp
SELECT
  SUM(x2.pay_amount) AS pay_amount,
  x1.glb_plf,
  x1.policy,
   x1.sort,
   x1.glb_dc,
  '${ADD_TIME}' as add_time
from
  tmp.cat_sku_user_country_tmp x1
  INNER JOIN tmp.cat_sku_user_tmp x2 ON x1.glb_u = x2.user_id
  AND x1.sku = x2.goods_sn
  where
  x2.order_status not in ('0', '11')
group by
  x1.glb_plf,
  x1.policy,
   x1.sort,
   x1.glb_dc
;

--商品收藏数
INSERT OVERWRITE TABLE tmp.cat_collect_tmp
SELECT count(*) as collect_num,
m.glb_plf,
m.policy,
m.sort,
m.glb_dc,
'${ADD_TIME}' as add_time
FROM
(
    SELECT
        glb_plf,
        get_json_object(glb_skuinfo, '$.sku') as sku,
        get_json_object(user_bh_order_seq,'$.policy') as policy,
        get_json_object(glb_filter,'$.sort') as sort,
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
group by
  m.glb_plf,
  m.policy,
  m.sort,
  m.glb_dc
;

CREATE TABLE IF NOT EXISTS tmp.zaful_cat_report(
pv                        INT            COMMENT "页面PV",
uv                        INT            COMMENT "页面UV",
exp_num                   INT            COMMENT "商品曝光数",
click_num                 INT            COMMENT "商品点击数",
exp_click_ratio           decimal(10,4)  COMMENT "曝光点击率",
cart_num                  INT            COMMENT "商品加购数",
cart_ratio                decimal(10,4)  COMMENT "加购率",
order_num                 INT            COMMENT "商品下单数",   
conversion_ratio          decimal(10,4)  COMMENT "下单转化率",
purchase_num              INT            COMMENT "支付订单数",  
pay_amount                decimal(10,4)  COMMENT "购买金额",
purchase_ratio            decimal(10,4)  COMMENT "购买转化率",
total_conversion_ratio    decimal(10,4)  COMMENT "总体转化率",
collect_num               INT            COMMENT "商品收藏数",
platform                  STRING         COMMENT "平台",
policy                    STRING         COMMENT "ABTEST",
sort                      STRING         COMMENT "sort",
glb_dc                    STRING         COMMENT "语言站"
)
COMMENT '分类列表数据报表'
PARTITIONED BY (add_time STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\u0001' LINES TERMINATED BY '\n' STORED AS TEXTFILE
;



INSERT overwrite TABLE tmp.zaful_cat_report PARTITION (add_time = '${ADD_TIME}')
select
  a.pv,
  b.uv,
  c.exp_num,
  d.click_num,
  d.click_num / c.exp_num,
  e.cart_num,
  e.cart_num / c.exp_num,
  f.order_num,
  f.order_num / c.exp_num,
  g.purchase_num,
  h.pay_amount,
  g.purchase_num / c.exp_num,
  g.purchase_num / c.exp_num,
  i.collect_num,
  a.glb_plf,
  a.policy,
  a.sort,
  a.glb_dc
from
  (select pv, add_time, glb_dc, sort, policy,glb_plf from tmp.cat_pv_tmp) a
 join 
  (select uv, add_time,  glb_dc, sort, policy,glb_plf from tmp.cat_uv_tmp) b 
ON a.add_time=b.add_time and a.glb_dc=b.glb_dc and a.sort=b.sort and a.policy=b.policy and a.glb_plf=b.glb_plf
 join 
  (select exp_num, add_time, glb_dc, sort, policy,glb_plf from tmp.cat_exp_tmp) c 
ON a.add_time=c.add_time and a.glb_dc=c.glb_dc  and a.sort=c.sort and a.policy=c.policy and a.glb_plf=c.glb_plf
 join 
  (select click_num, add_time,  glb_dc, sort, policy,glb_plf from tmp.cat_click_tmp ) d 
ON a.add_time=d.add_time and a.glb_dc=d.glb_dc  and a.sort=d.sort and a.policy=d.policy and a.glb_plf=d.glb_plf
 join 
  (select cart_num, add_time,  glb_dc, sort, policy,glb_plf from tmp.cat_cart_tmp ) e 
ON a.add_time=e.add_time and a.glb_dc=e.glb_dc  and a.sort=e.sort and a.policy=e.policy and a.glb_plf=e.glb_plf
 join 
  (select order_num, add_time, glb_dc, sort, policy,glb_plf from tmp.cat_order_tmp ) f 
ON a.add_time=f.add_time and a.glb_dc=f.glb_dc  and a.sort=f.sort and a.policy=f.policy and a.glb_plf=f.glb_plf
 join 
  (select purchase_num, add_time,  glb_dc, sort, policy,glb_plf from tmp.cat_purchase_tmp ) g 
ON a.add_time=g.add_time and a.glb_dc=g.glb_dc and a.sort=g.sort and a.policy=g.policy and a.glb_plf=g.glb_plf
 join 
  (select pay_amount, add_time, glb_dc, sort, policy,glb_plf from tmp.cat_pay_amount_tmp) h 
ON a.add_time=h.add_time and a.glb_dc=h.glb_dc and a.sort=h.sort and a.policy=h.policy and a.glb_plf=h.glb_plf
 join 
  (select collect_num, add_time, glb_dc, sort, policy,glb_plf from tmp.cat_collect_tmp ) i
ON a.add_time=i.add_time and a.glb_dc=i.glb_dc and a.sort=i.sort and a.policy=i.policy and a.glb_plf=i.glb_plf
;






