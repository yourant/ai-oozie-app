
--@author wuchao
--@date 2018年11月05日 
--@desc  Zaful分类列表页报表

SET mapred.job.name=rewrite_zaful_cat_report_AB;
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






--GMV	
CREATE TABLE IF NOT EXISTS dw_zaful_recommend.zaful_pc_wuc_gmv(
pay_gmv                 INT          COMMENT "GMV",
add_time                  STRING         COMMENT "时间"
)
COMMENT "GMV"
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\u0001' LINES TERMINATED BY '\n' STORED AS TEXTFILE;

CREATE TABLE IF NOT EXISTS dw_zaful_recommend.rewrite_cat_sku_user_country_tmp_nopolicy_wuc(
sku                 STRING          COMMENT "--中间表report_sku_user_tmp",
glb_u               STRING          COMMENT "user_id"
)
COMMENT "中间表report_sku_user_tmp"
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\u0001' LINES TERMINATED BY '\n' STORED AS TEXTFILE;

CREATE TABLE IF NOT EXISTS dw_zaful_recommend.report_sku_user_tmp_wuc_gmv(
goods_sn                  STRING         COMMENT "sku",
user_id                   STRING         COMMENT "用户ID",
order_status              STRING         COMMENT "订单状态",
pay_amount                decimal(10,2)  COMMENT "购买金额",
goods_number              STRING         COMMENT "商品数量"
)
COMMENT "推荐位报表user-sku中间表"
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\u0001' LINES TERMINATED BY '\n' STORED AS TEXTFILE;



--中间表report_sku_user_tmp：取sku,user_id
INSERT OVERWRITE TABLE dw_zaful_recommend.rewrite_cat_sku_user_country_tmp_nopolicy_wuc
SELECT
  m.sku,
  n.glb_u
FROM
  (
    SELECT
      glb_od,
      regexp_extract(glb_skuinfo, '(.*?sku":")([0-9a-zA-Z]*)(".*?)', 2) AS sku,
      glb_dc
    FROM
      stg.zf_pc_event_info
    WHERE
      concat_ws('-', year, month, day) BETWEEN '${ADD_TIME_W}'
      AND '${ADD_TIME}'
      AND glb_t = 'ic'
      AND glb_x = 'ADT'
      and get_json_object(glb_ubcta, '$.fmd')='mr_T_9'
      AND glb_plf='pc'
  ) m
  INNER JOIN dw_zaful_recommend.zaful_od_u_map n ON m.glb_od = n.glb_od
GROUP BY
  m.sku,
  n.glb_u
;


INSERT OVERWRITE TABLE dw_zaful_recommend.report_sku_user_tmp_wuc_gmv
SELECT
  p.goods_sn,
  x.user_id,
  x.order_status,
  p.pay_amount,
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
  JOIN   (
  SELECT goods_sn,order_id,
  goods_number,
  case when goods_pay_amount <> '0' then goods_pay_amount
  else goods_price*goods_number end as pay_amount
  from
   stg.zaful_eload_order_goods ) p
  ON x.order_id = p.order_id
group by
  p.goods_sn,
  x.user_id,
  x.order_status,
  p.pay_amount,
  p.goods_number
;











--GMV
INSERT INTO TABLE  dw_zaful_recommend.zaful_pc_wuc_gmv
SELECT
  SUM(x2.pay_amount) AS pay_amount,
  '${ADD_TIME}' as add_time
from
  dw_zaful_recommend.rewrite_cat_sku_user_country_tmp_nopolicy_wuc x1
  INNER JOIN dw_zaful_recommend.report_sku_user_tmp_wuc_gmv x2 ON x1.glb_u = x2.user_id
  AND x1.sku = x2.goods_sn

;







--结束了
