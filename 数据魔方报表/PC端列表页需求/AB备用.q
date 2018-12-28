--实验样本量
INSERT OVERWRITE TABLE  tmp.rewrite_sample_num_tmp
SELECT
   count(distinct n.glb_od) AS sample_num,
   n.policy,
   n.glb_dc,
   '${ADD_TIME}' as add_time
FROM(
    SELECT
      glb_od,
      get_json_object(user_bh_order_seq,'$.policy') as policy,
      glb_dc,
      concat_ws('-', year, month, day) as add_time
    FROM
      stg.zf_pc_event_info
    WHERE
      concat_ws('-', year, month, day) = '${ADD_TIME}'
      and glb_s = 'b01'
      and glb_plf = 'pc'
	  and get_json_object(glb_filter,'$.sort')='Recommend'
    ) n
group by
  n.policy,
  n.glb_dc,
  ;






--商品曝光数
INSERT OVERWRITE TABLE  tmp.rewrite_exp_num_tmp
SELECT
   SUM(n.exp_num) AS exp_num,
   n.policy,
   n.glb_dc,
   '${ADD_TIME}' as add_time
FROM(
    SELECT
      length(get_json_object(glb_ubcta, '$.sku'))-length(regexp_replace(get_json_object(glb_ubcta, '$.sku'),',',''))+1 as exp_num,
      get_json_object(user_bh_order_seq,'$.policy') as policy,
	  glb_dc,
      concat_ws('-', year, month, day) as add_time
    FROM
      stg.zf_pc_event_info
    WHERE
      concat_ws('-', year, month, day) = '${ADD_TIME}'
      and glb_s = 'b01'
      and glb_plf = 'pc'
	  and get_json_object(glb_filter,'$.sort')='Recommend'
	  and glb_t='ie'
    ) n
group by
  n.policy,
  n.glb_dc
  ;

  
  
--商品点击数
INSERT OVERWRITE TABLE  tmp.rewrite_click_num_tmp
SELECT
  count(glb_od) AS click_num,
  get_json_object(user_bh_order_seq,'$.policy') as policy,
  glb_dc,
  '${ADD_TIME}' as add_time
FROM
  stg.zf_pc_event_info
WHERE
  concat_ws('-', year, month, day)  ='${ADD_TIME}'
  and glb_s = 'b01'
  and glb_plf = 'pc' 
  and get_json_object(glb_filter,'$.sort')='Recommend'
  and glb_t='ic'
  AND glb_x in ('sku','addtobag')
  and get_json_object(glb_ubcta, '$.sckw') is null
group by
  get_json_object(user_bh_order_seq,'$.policy'),
  glb_dc
;



--商品加购数
INSERT OVERWRITE TABLE  tmp.rewrite_cart_num_tmp
SELECT 
SUM(m.pam) as cart_num,
m.policy,
m.glb_dc,
'${ADD_TIME}' as add_time
FROM
(
    SELECT
        get_json_object(glb_skuinfo, '$.pam') as pam,
        get_json_object(user_bh_order_seq,'$.policy') as policy,
         glb_dc,
         concat_ws('-', year, month, day) as add_time
    FROM
        stg.zf_pc_event_info
    WHERE
        concat_ws('-', year, month, day) ='${ADD_TIME}'
        AND glb_t = 'ic'
        AND glb_x = 'ADT'
        and get_json_object(glb_ubcta, '$.fmd')='mp'
        and get_json_object(glb_ubcta, '$.sckw') is null
        AND glb_plf='pc'
) m 
group by
  m.policy,
  m.glb_dc
;


--========================================================================================
--中间表report_sku_user_tmp：取sku,user_id
INSERT OVERWRITE TABLE tmp.rewrite_cat_sku_user_country_tmp
SELECT
  m.sku,
  n.glb_u,
  m.policy,
  m.glb_dc
FROM
  (
    SELECT
      glb_od,
      regexp_extract(glb_skuinfo, '(.*?sku":")([0-9a-zA-Z]*)(".*?)', 2) AS sku,
      get_json_object(user_bh_order_seq,'$.policy') as policy,
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
      AND glb_plf='pc'
  ) m
  INNER JOIN dw_zaful_recommend.zaful_od_u_map n ON m.glb_od = n.glb_od
GROUP BY
  m.sku,
  n.glb_u,
  m.policy,
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



--销量
INSERT OVERWRITE TABLE  tmp.rewrite_purchase_num_tmp
SELECT
  SUM(x2.goods_number) AS purchase_num,
  x1.policy,
   x1.glb_dc,
  '${ADD_TIME}' as add_time
from
  tmp.rewrite_cat_sku_user_country_tmp x1
  INNER JOIN tmp.cat_sku_user_tmp x2 ON x1.glb_u = x2.user_id
  AND x1.sku = x2.goods_sn
  where
  x2.order_status not in ('0', '11')
group by
  x1.policy,
   x1.glb_dc
;






--销售额
INSERT OVERWRITE TABLE  tmp.rewrite_pay_amount_tmp
SELECT
  SUM(x2.pay_amount) AS pay_amount,
  x1.policy,
   x1.glb_dc,
  '${ADD_TIME}' as add_time
from
  tmp.rewrite_cat_sku_user_country_tmp x1
  INNER JOIN tmp.cat_sku_user_tmp x2 ON x1.glb_u = x2.user_id
  AND x1.sku = x2.goods_sn
  where
  x2.order_status not in ('0', '11')
group by
  x1.policy,
   x1.glb_dc
;

--GMV
INSERT OVERWRITE TABLE  tmp.rewrite_pay_gmv_tmp
SELECT
  SUM(x2.pay_amount) AS pay_amount,
  x1.policy,
   x1.glb_dc,
  '${ADD_TIME}' as add_time
from
  tmp.rewrite_cat_sku_user_country_tmp x1
  INNER JOIN tmp.cat_sku_user_tmp x2 ON x1.glb_u = x2.user_id
  AND x1.sku = x2.goods_sn
group by
  x1.policy,
   x1.glb_dc
;


INSERT overwrite TABLE tmp.rewrite_zaful_cat_report PARTITION (add_time = '${ADD_TIME}')
select 
    a.sample_num,
	b.exp_num,
	c.click_num,
	c.click_num / b.exp_num,
	d.cart_num,
	d.cart_num / b.exp_num,
	e.purchase_num,
	e.purchase_num / b.exp_num,
	f.pay_amount,
	g.pay_gmv
from 
    (select '${ADD_TIME}' as add_time) mm 
	left join
    (select sample_num,policy,glb_dc,add_time from tmp.rewrite_sample_num_tmp where policy='A') a on mm.add_time=a.add_time
    left join
    ((select exp_num,policy,glb_dc,add_time from tmp.rewrite_sample_num_tmp where policy='A') b on mm.add_time=b.add_time)
	left join
    ((select click_num,policy,glb_dc,add_time from tmp.rewrite_sample_num_tmp where policy='A') c on mm.add_time=c.add_time)
	left join
    ((select cart_num,policy,glb_dc,add_time from tmp.rewrite_sample_num_tmp where policy='A') d on mm.add_time=d.add_time)
	left join
    ((select purchase_num,policy,glb_dc,add_time from tmp.rewrite_sample_num_tmp where policy='A') e on mm.add_time=e.add_time)
	left join
    ((select pay_amount,policy,glb_dc,add_time from tmp.rewrite_sample_num_tmp where policy='A') f on mm.add_time=f.add_time)
	left join
    ((select pay_gmv,policy,glb_dc,add_time from tmp.rewrite_sample_num_tmp where policy='A') g on mm.add_time=g.add_time)
union all
select 
    a.sample_num,
	b.exp_num,
	c.click_num,
	c.click_num / b.exp_num,
	d.cart_num,
	d.cart_num / b.exp_num,
	e.purchase_num,
	e.purchase_num / b.exp_num,
	f.pay_amount,
	g.pay_gmv
from 
    (select '${ADD_TIME}' as add_time) mm 
	left join
    (select sample_num,policy,glb_dc,add_time from tmp.rewrite_sample_num_tmp where policy='B') a on mm.add_time=a.add_time
    left join
    ((select exp_num,policy,glb_dc,add_time from tmp.rewrite_sample_num_tmp where policy='B') b on mm.add_time=b.add_time)
	left join
    ((select click_num,policy,glb_dc,add_time from tmp.rewrite_sample_num_tmp where policy='B') c on mm.add_time=c.add_time)
	left join
    ((select cart_num,policy,glb_dc,add_time from tmp.rewrite_sample_num_tmp where policy='B') d on mm.add_time=d.add_time)
	left join
    ((select purchase_num,policy,glb_dc,add_time from tmp.rewrite_sample_num_tmp where policy='B') e on mm.add_time=e.add_time)
	left join
    ((select pay_amount,policy,glb_dc,add_time from tmp.rewrite_sample_num_tmp where policy='B') f on mm.add_time=f.add_time)
	left join
    ((select pay_gmv,policy,glb_dc,add_time from tmp.rewrite_sample_num_tmp where policy='B') g on mm.add_time=g.add_time)



























































