set mapred.job.queue.name=root.ai.online; 
insert overwrite TABLE dw_zaful_recommend.zf_pc_event_info_detail_exp
PARTITION (date = '${DATE}')
SELECT
get_json_object(glb_skuinfo, '$.sku') AS goods_sn, 
  glb_oi,
  CASE WHEN glb_u='' THEN 0 ELSE glb_u END AS glb_u,
  glb_od,
  glb_w,
  glb_x,
  glb_tm,
  year,
  month,
  day,
  unix_timestamp(concat(year, month, day),'yyyyMMdd') AS add_time
FROM
  stg.zf_pc_event_info
WHERE
  concat(year, month, day) = '${DATE}'
  and glb_t='ic' and glb_skuinfo <> ''
  AND regexp_extract (glb_skuinfo,'(.?)',1) <> '['
;



insert overwrite TABLE dw_zaful_recommend.zaful_eload_order_detail_exp
PARTITION (date = '${DATE}')
SELECT
  a.order_id,
  b.goods_sn,
  a.user_id,
  a.add_time
FROM
  (SELECT order_id,user_id,add_time from 
  stg.zaful_eload_order_info 
  WHERE order_status <> 0
  and from_unixtime(add_time, 'yyyyMMdd') ='${DATE}'
  ) a
  join 
  (SELECT order_id,goods_sn from
  stg.zaful_eload_order_goods
  WHERE from_unixtime(addtime, 'yyyyMMdd') ='${DATE}'
  ) b on a.order_id = b.order_id
;
