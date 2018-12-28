
--@author wuchao
--@date 2018年10月17日 
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






CREATE TABLE IF NOT EXISTS tmp.rewrite_zaful_cat_report(
sample_num                INT            COMMENT "实验样本量",
exp_num                   INT            COMMENT "商品曝光数",
click_num                 INT            COMMENT "商品点击数",
exp_click_ratio           decimal(10,5)  COMMENT "曝光点击率",
cart_num                  INT            COMMENT "商品加购数",
cart_ratio                decimal(10,5)  COMMENT "加购率",
purchase_num              INT            COMMENT "销量",  
purchase_ratio            decimal(10,5)  COMMENT "购买转化率",
pay_amount                INT            COMMENT "销售额",
pay_gmv                   INT            COMMENT "GMV",
policy                    STRING         COMMENT "ABTEST",
glb_dc                    STRING         COMMENT "语言站"
)
COMMENT 'Z网列表页排序AB测试数据报表'
PARTITIONED BY (add_time STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\u0001' LINES TERMINATED BY '\n' STORED AS TEXTFILE
;


--实验样本量	
CREATE TABLE IF NOT EXISTS tmp.rewrite_sample_num_tmp(
sample_num                INT            COMMENT "实验样本量",
policy                    STRING         COMMENT "ABTEST",
glb_dc                    STRING         COMMENT "语言站",
add_time                  STRING         COMMENT "时间"
)
COMMENT "实验样本量"
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\u0001' LINES TERMINATED BY '\n' STORED AS TEXTFILE;

--商品曝光数	
CREATE TABLE IF NOT EXISTS tmp.rewrite_exp_num_tmp(
exp_num                INT            COMMENT "商品曝光数",
policy                    STRING         COMMENT "ABTEST",
glb_dc                    STRING         COMMENT "语言站",
add_time                  STRING         COMMENT "时间"
)
COMMENT "商品曝光数"
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\u0001' LINES TERMINATED BY '\n' STORED AS TEXTFILE;


--商品点击数	
CREATE TABLE IF NOT EXISTS tmp.rewrite_click_num_tmp(
click_num                 INT          COMMENT "商品点击数",
policy                    STRING         COMMENT "ABTEST",
glb_dc                    STRING         COMMENT "语言站",
add_time                  STRING         COMMENT "时间"
)
COMMENT "商品点击数"
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\u0001' LINES TERMINATED BY '\n' STORED AS TEXTFILE;




--商品加购数	
CREATE TABLE IF NOT EXISTS tmp.rewrite_cart_num_tmp(
cart_num                 INT          COMMENT "商品加购数",
policy                    STRING         COMMENT "ABTEST",
glb_dc                    STRING         COMMENT "语言站",
add_time                  STRING         COMMENT "时间"
)
COMMENT "商品加购数"
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\u0001' LINES TERMINATED BY '\n' STORED AS TEXTFILE;

----中间表report_sku_user_tmp：取sku,user_id
CREATE TABLE IF NOT EXISTS tmp.rewrite_cat_sku_user_country_tmp(
sku                 STRING          COMMENT "--中间表report_sku_user_tmp",
glb_u               STRING          COMMENT "user_id",
policy                    STRING         COMMENT "ABTEST",
glb_dc                    STRING         COMMENT "语言站"
)
COMMENT "中间表report_sku_user_tmp"
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\u0001' LINES TERMINATED BY '\n' STORED AS TEXTFILE;


--销量	
CREATE TABLE IF NOT EXISTS tmp.rewrite_purchase_num_tmp(
purchase_num                 INT          COMMENT "销量",
policy                    STRING         COMMENT "ABTEST",
glb_dc                    STRING         COMMENT "语言站",
add_time                  STRING         COMMENT "时间"
)
COMMENT "销量"
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\u0001' LINES TERMINATED BY '\n' STORED AS TEXTFILE;



--销售额	
CREATE TABLE IF NOT EXISTS tmp.rewrite_pay_amount_tmp(
pay_amount                 INT          COMMENT "销售额",
policy                    STRING         COMMENT "ABTEST",
glb_dc                    STRING         COMMENT "语言站",
add_time                  STRING         COMMENT "时间"
)
COMMENT "销售额"
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\u0001' LINES TERMINATED BY '\n' STORED AS TEXTFILE;


--GMV	
CREATE TABLE IF NOT EXISTS tmp.rewrite_pay_gmv_tmp(
pay_gmv                 INT          COMMENT "GMV",
policy                    STRING         COMMENT "ABTEST",
glb_dc                    STRING         COMMENT "语言站",
add_time                  STRING         COMMENT "时间"
)
COMMENT "GMV"
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\u0001' LINES TERMINATED BY '\n' STORED AS TEXTFILE;


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
  n.glb_dc
  ;






--商品曝光数
INSERT OVERWRITE TABLE  tmp.rewrite_exp_num_tmp
SELECT
   count(*) AS exp_num,
   n.policy,
   n.glb_dc,
   '${ADD_TIME}' as add_time
FROM
  (
    SELECT
      log_id,
      get_json_object(glb_ubcta_col, '$.sku') as sku,
      concat_ws('-', year, month, day) as add_time
    FROM
      stg.zf_pc_event_ubcta_info
    WHERE
      concat_ws('-', year, month, day) ='${ADD_TIME}'
      AND get_json_object(glb_ubcta_col, '$.sku') <> ''
  ) m
  INNER JOIN (
    SELECT
      log_id,
      get_json_object(user_bh_order_seq,'$.policy') as policy,
      glb_dc,
      concat_ws('-', year, month, day) as add_time
    FROM
      stg.zf_pc_event_info
    WHERE
      concat_ws('-', year, month, day)  ='${ADD_TIME}'
      and glb_s = 'b01'
      and glb_plf = 'pc'
	  and get_json_object(glb_filter,'$.sort')='Recommend'
	  and glb_t='ie'
  ) n ON m.log_id = n.log_id and m.add_time = n.add_time
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


INSERT into TABLE tmp.rewrite_zaful_cat_report PARTITION (add_time = '${ADD_TIME}')
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
	g.pay_gmv,
	'A',
	a.glb_dc
from 
    (select sample_num,policy,glb_dc,add_time from tmp.rewrite_sample_num_tmp where policy='A') a 
    join
    (select exp_num,policy,glb_dc,add_time from tmp.rewrite_exp_num_tmp where policy='A') b 
	on a.add_time=b.add_time and a.glb_dc=b.glb_dc
	join
    (select click_num,policy,glb_dc,add_time from tmp.rewrite_click_num_tmp where policy='A') c 
	on a.add_time=c.add_time and a.glb_dc=c.glb_dc
	 join
    (select cart_num,policy,glb_dc,add_time from tmp.rewrite_cart_num_tmp where policy='A') d 
	on a.add_time=d.add_time and a.glb_dc=d.glb_dc
	 join
    (select purchase_num,policy,glb_dc,add_time from tmp.rewrite_purchase_num_tmp where policy='A') e 
	on a.add_time=e.add_time and a.glb_dc=e.glb_dc
	 join
    (select pay_amount,policy,glb_dc,add_time from tmp.rewrite_pay_amount_tmp where policy='A') f 
	on a.add_time=f.add_time and a.glb_dc=f.glb_dc
	 join
    (select pay_gmv,policy,glb_dc,add_time from tmp.rewrite_pay_gmv_tmp where policy='A') g 
	on a.add_time=g.add_time and a.glb_dc=g.glb_dc
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
	g.pay_gmv,
	'B',
	a.glb_dc
from 
    (select sample_num,policy,glb_dc,add_time from tmp.rewrite_sample_num_tmp where policy='B') a 
    join
    (select exp_num,policy,glb_dc,add_time from tmp.rewrite_exp_num_tmp where policy='B') b 
	on a.add_time=b.add_time and a.glb_dc=b.glb_dc
	join
    (select click_num,policy,glb_dc,add_time from tmp.rewrite_click_num_tmp where policy='B') c 
	on a.add_time=c.add_time and a.glb_dc=c.glb_dc
	 join
    (select cart_num,policy,glb_dc,add_time from tmp.rewrite_cart_num_tmp where policy='B') d 
	on a.add_time=d.add_time and a.glb_dc=d.glb_dc
	 join
    (select purchase_num,policy,glb_dc,add_time from tmp.rewrite_purchase_num_tmp where policy='B') e 
	on a.add_time=e.add_time and a.glb_dc=e.glb_dc
	 join
    (select pay_amount,policy,glb_dc,add_time from tmp.rewrite_pay_amount_tmp where policy='B') f 
	on a.add_time=f.add_time and a.glb_dc=f.glb_dc
	 join
    (select pay_gmv,policy,glb_dc,add_time from tmp.rewrite_pay_gmv_tmp where policy='B') g 
	on a.add_time=g.add_time and a.glb_dc=g.glb_dc






