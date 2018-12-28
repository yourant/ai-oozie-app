--@author wuchao
--@date 2018年11月24日 
--@desc  RG PC/M推荐位报表

SET mapred.job.name=rosegal_recommend_position_report;
set mapred.job.queue.name=root.ai.offline;
SET mapred.max.split.size=128000000;
SET mapred.min.split.size=64000000;
SET mapred.min.split.size.per.node=64000000;
SET mapred.min.split.size.per.rack=64000000;
SET hive.exec.reducers.bytes.per.reducer = 128000000;
SET hive.merge.mapfiles=true;
SET hive.merge.mapredfiles= true;
SET hive.input.format=org.apache.hadoop.hive.ql.io.CombineHiveInputFormat;
SET hive.merge.size.per.task=256000000;
SET hive.exec.parallel = true; 



















--页面PV
INSERT OVERWRITE TABLE dw_proj.rosegal_pc_report_pv_tmp
SELECT
  count(cookie_id) as pv,
  platform,
  'related_recommendations' as recommend_position,
  country_number,
  country_name,
  '${ADD_TIME}' as add_time
FROM
  ods.ods_pc_burial_log
WHERE
  concat_ws('-', year, month, day) = '${ADD_TIME}'
  and behaviour_type = 'ie'
  and page_main_type = 'c'
  and sub_event_field is null
  and site='rosegal'
group by
  platform,
  country_number,
  country_name
union all
SELECT
  count(cookie_id) as pv,
  platform,
  'customers_also_viewed' as recommend_position,
  country_number,
  country_name,
  '${ADD_TIME}' as add_time
FROM
  ods.ods_pc_burial_log
WHERE
  concat_ws('-', year, month, day) = '${ADD_TIME}'
  and behaviour_type = 'ie'
  and page_main_type = 'c'
  and sub_event_field is null
  and site='rosegal'
group by
  platform,
  country_number,
  country_name
union all
SELECT
  count(cookie_id) as pv,
  platform,
  'featured_recommendations' as recommend_position,
  country_number,
  country_name,
  '${ADD_TIME}' as add_time
FROM
  ods.ods_pc_burial_log
WHERE
  concat_ws('-', year, month, day) = '${ADD_TIME}'
  and behaviour_type = 'ie'
  and page_sub_type = 'd05'
  and sub_event_field is null
  and site='rosegal'
group by
  platform,
  country_number,
  country_name
union all
SELECT
  count(cookie_id) as pv,
  platform,
  'may_be_you_like' as recommend_position,
  country_number,
  country_name,
  '${ADD_TIME}' as add_time
FROM
  ods.ods_pc_burial_log
WHERE
  concat_ws('-', year, month, day) = '${ADD_TIME}'
  and behaviour_type = 'ie'
  and page_sub_type = 'b02'
  and sub_event_field is null
  and get_json_object(page_info,'$.view') = 0
  and site='rosegal'
group by
  platform,
  country_number,
  country_name
;




--页面UV	
INSERT OVERWRITE TABLE dw_proj.rosegal_pc_report_uv_tmp
SELECT
  count(distinct cookie_id) as pv,
  platform,
  'related_recommendations' as recommend_position,
  country_number,
  country_name,
  '${ADD_TIME}' as add_time
FROM
  ods.ods_pc_burial_log
WHERE
  concat_ws('-', year, month, day) = '${ADD_TIME}'
  and behaviour_type = 'ie'
  and page_main_type = 'c'
  and sub_event_field is null
  and site='rosegal'
group by
  platform,
  country_number,
  country_name
union all
SELECT
  count(distinct cookie_id) as pv,
  platform,
  'customers_also_viewed' as recommend_position,
  country_number,
  country_name,
  '${ADD_TIME}' as add_time
FROM
  ods.ods_pc_burial_log
WHERE
  concat_ws('-', year, month, day) = '${ADD_TIME}'
  and behaviour_type = 'ie'
  and page_main_type = 'c'
  and sub_event_field is null
  and site='rosegal'
group by
  platform,
  country_number,
  country_name
union all
SELECT
  count(distinct cookie_id) as pv,
  platform,
  'featured_recommendations' as recommend_position,
  country_number,
  country_name,
  '${ADD_TIME}' as add_time
FROM
  ods.ods_pc_burial_log
WHERE
  concat_ws('-', year, month, day) = '${ADD_TIME}'
  and behaviour_type = 'ie'
  and page_sub_type = 'd05'
  and sub_event_field is null
  and site='rosegal'
group by
  platform,
  country_number,
  country_name
union all
SELECT
  count(distinct cookie_id) as pv,
  platform,
  'may_be_you_like' as recommend_position,
  country_number,
  country_name,
  '${ADD_TIME}' as add_time
FROM
  ods.ods_pc_burial_log
WHERE
  concat_ws('-', year, month, day) = '${ADD_TIME}'
  and behaviour_type = 'ie'
  and page_sub_type = 'b02'
  and sub_event_field is null
  and get_json_object(page_info,'$.view') = 0
  and site='rosegal'
group by
  platform,
  country_number,
  country_name
;

--商品曝光数	
INSERT OVERWRITE TABLE dw_proj.rosegal_pc_report_exp_num_tmp
SELECT
sum(m.exp_num) as exp_num,
m.platform,
m.recommend_position,
m.country_number,
m.country_name,
'${ADD_TIME}' as add_time
FROM
 (
    SELECT
    length(get_json_object(x.sku, '$.sku'))-length(regexp_replace(get_json_object(x.sku, '$.sku'),',',''))+1 as exp_num,
    x.platform,
    get_json_object(x.sku,'$.mrlc') as recommend_position,
    x.country_number,
    x.country_name
    FROM
    (
      SELECT 
      sku,
      n.platform,
      n.country_number,
      n.country_name
      FROM
      (
        SELECT
          platform,
          regexp_replace(regexp_extract(sub_event_field ,'^\\[(.+)\\]$',1),'\\}\\,\\{','\\}\\|\\|\\{') as skus,
          country_number,
          country_name
        FROM
          ods.ods_pc_burial_log
        WHERE
          concat_ws('-', year, month, day) = '${ADD_TIME}'
          and behaviour_type = 'ie'
          and page_main_type = 'c'
          and page_module = 'mr'
          --and get_json_object(regexp_replace(regexp_extract(sub_event_field ,'^\\[(.+)\\]$',1),'\\}\\,\\{(.+)\\}','}'),'$.mrlc') in ('T_2','T_3','T_4','T_5','T_6')
          and site='rosegal'
      ) n
        LATERAL VIEW explode(split(n.skus,'\\|\\|')) zqms as sku
    ) x
    where get_json_object(x.sku,'$.mrlc') in ('T_2','T_3','T_4','T_5','T_6')
 ) m
group by
  m.platform,
  m.recommend_position,
  m.country_number,
  m.country_name

union all

SELECT
sum(m.exp_num) as exp_num,
m.platform,
m.recommend_position,
m.country_number,
m.country_name,
'${ADD_TIME}' as add_time
FROM
 (
    SELECT
    length(get_json_object(x.sku, '$.sku'))-length(regexp_replace(get_json_object(x.sku, '$.sku'),',',''))+1 as exp_num,
    x.platform,
    get_json_object(x.sku,'$.mrlc') as recommend_position,
    x.country_number,
    x.country_name
    FROM
    (
      SELECT 
      sku,
      n.platform,
      n.country_number,
      n.country_name
      FROM
      (
        SELECT
          platform,
          regexp_replace(regexp_extract(sub_event_field ,'^\\[(.+)\\]$',1),'\\}\\,\\{','\\}\\|\\|\\{') as skus,
          country_number,
          country_name
        FROM
          ods.ods_pc_burial_log
        WHERE
          concat_ws('-', year, month, day) = '${ADD_TIME}'
          and behaviour_type = 'ie'
          and page_main_type = 'd'
          and page_module = 'mr'
          --and get_json_object(regexp_replace(regexp_extract(sub_event_field ,'^\\[(.+)\\]$',1),'\\}\\,\\{(.+)\\}','}'),'$.mrlc') in ('T_2','T_3','T_4','T_5','T_6')
          and site='rosegal'
      ) n
        LATERAL VIEW explode(split(n.skus,'\\|\\|')) zqms as sku
    ) x
    where get_json_object(x.sku,'$.mrlc') in ('T_2','T_3','T_4','T_5','T_6')
 ) m
group by
  m.platform,
  m.recommend_position,
  m.country_number,
  m.country_name

union all 

SELECT
sum(m.exp_num) as exp_num,
m.platform,
m.recommend_position,
m.country_number,
m.country_name,
'${ADD_TIME}' as add_time
FROM
 (
    SELECT
    length(get_json_object(x.sku, '$.sku'))-length(regexp_replace(get_json_object(x.sku, '$.sku'),',',''))+1 as exp_num,
    x.platform,
    get_json_object(x.sku,'$.mrlc') as recommend_position,
    x.country_number,
    x.country_name
    FROM
    (
      SELECT 
      sku,
      n.platform,
      n.country_number,
      n.country_name
      FROM
      (
        SELECT
          platform,
          regexp_replace(regexp_extract(sub_event_field ,'^\\[(.+)\\]$',1),'\\}\\,\\{','\\}\\|\\|\\{') as skus,
          country_number,
          country_name
        FROM
          ods.ods_pc_burial_log
        WHERE
          concat_ws('-', year, month, day) = '${ADD_TIME}'
          and behaviour_type = 'ie'
          and page_sub_type = 'b02'
          and page_module = 'mr'
          --and get_json_object(regexp_replace(regexp_extract(sub_event_field ,'^\\[(.+)\\]$',1),'\\}\\,\\{(.+)\\}','}'),'$.mrlc') in ('T_2','T_3','T_4','T_5','T_6')
          and site='rosegal'
      ) n
        LATERAL VIEW explode(split(n.skus,'\\|\\|')) zqms as sku
    ) x
    where get_json_object(x.sku,'$.mrlc') in ('T_2','T_3','T_4','T_5','T_6')
 ) m
group by
  m.platform,
  m.recommend_position,
  m.country_number,
  m.country_name
;



--查看商品UV
INSERT OVERWRITE TABLE dw_proj.rosegal_pc_report_sku_uv_tmp
SELECT
count(distinct m.cookie_id) as sku_uv,
m.platform,
m.recommend_position,
m.country_number,
m.country_name,
'${ADD_TIME}' as add_time
FROM
 (
    SELECT
    x.cookie_id,
    x.platform,
    get_json_object(x.sku,'$.mrlc') as recommend_position,
    x.country_number,
    x.country_name
    FROM
    (
      SELECT 
      sku,
      n.cookie_id,
      n.platform,
      n.country_number,
      n.country_name
      FROM
      (
        SELECT
          cookie_id,
          platform,
          regexp_replace(regexp_extract(sub_event_field ,'^\\[(.+)\\]$',1),'\\}\\,\\{','\\}\\|\\|\\{') as skus,
          country_number,
          country_name
        FROM
          ods.ods_pc_burial_log
        WHERE
          concat_ws('-', year, month, day) = '${ADD_TIME}'
          and behaviour_type = 'ie'
          and page_main_type = 'c'
          and page_module = 'mr'
          --and get_json_object(regexp_replace(regexp_extract(sub_event_field ,'^\\[(.+)\\]$',1),'\\}\\,\\{(.+)\\}','}'),'$.mrlc') in ('T_2','T_3','T_4','T_5','T_6')
          and site='rosegal'
      ) n
        LATERAL VIEW explode(split(n.skus,'\\|\\|')) zqms as sku
    ) x
    where get_json_object(x.sku,'$.mrlc') in ('T_2','T_3','T_4','T_5','T_6')
 ) m
group by
  m.platform,
  m.recommend_position,
  m.country_number,
  m.country_name

union all

SELECT
count(distinct m.cookie_id) as sku_uv,
m.platform,
m.recommend_position,
m.country_number,
m.country_name,
'${ADD_TIME}' as add_time
FROM
 (
    SELECT
    x.cookie_id,
    x.platform,
    get_json_object(x.sku,'$.mrlc') as recommend_position,
    x.country_number,
    x.country_name
    FROM
    (
      SELECT 
      sku,
      n.cookie_id,
      n.platform,
      n.country_number,
      n.country_name
      FROM
      (
        SELECT
          cookie_id,
          platform,
          regexp_replace(regexp_extract(sub_event_field ,'^\\[(.+)\\]$',1),'\\}\\,\\{','\\}\\|\\|\\{') as skus,
          country_number,
          country_name
        FROM
          ods.ods_pc_burial_log
        WHERE
          concat_ws('-', year, month, day) = '${ADD_TIME}'
          and behaviour_type = 'ie'
          and page_main_type = 'd'
          and page_module = 'mr'
          --and get_json_object(regexp_replace(regexp_extract(sub_event_field ,'^\\[(.+)\\]$',1),'\\}\\,\\{(.+)\\}','}'),'$.mrlc') in ('T_2','T_3','T_4','T_5','T_6')
          and site='rosegal'
      ) n
        LATERAL VIEW explode(split(n.skus,'\\|\\|')) zqms as sku
    ) x
    where get_json_object(x.sku,'$.mrlc') in ('T_2','T_3','T_4','T_5','T_6')
 ) m
group by
  m.platform,
  m.recommend_position,
  m.country_number,
  m.country_name

union all

SELECT
count(distinct m.cookie_id) as sku_uv,
m.platform,
m.recommend_position,
m.country_number,
m.country_name,
'${ADD_TIME}' as add_time
FROM
 (
    SELECT
    x.cookie_id,
    x.platform,
    get_json_object(x.sku,'$.mrlc') as recommend_position,
    x.country_number,
    x.country_name
    FROM
    (
      SELECT 
      sku,
      n.cookie_id,
      n.platform,
      n.country_number,
      n.country_name
      FROM
      (
        SELECT
          cookie_id,
          platform,
          regexp_replace(regexp_extract(sub_event_field ,'^\\[(.+)\\]$',1),'\\}\\,\\{','\\}\\|\\|\\{') as skus,
          country_number,
          country_name
        FROM
          ods.ods_pc_burial_log
        WHERE
          concat_ws('-', year, month, day) = '${ADD_TIME}'
          and behaviour_type = 'ie'
          and page_sub_type = 'b02'
          and page_module = 'mr'
          --and get_json_object(regexp_replace(regexp_extract(sub_event_field ,'^\\[(.+)\\]$',1),'\\}\\,\\{(.+)\\}','}'),'$.mrlc') in ('T_2','T_3','T_4','T_5','T_6')
          and site='rosegal'
      ) n
        LATERAL VIEW explode(split(n.skus,'\\|\\|')) zqms as sku
    ) x
    where get_json_object(x.sku,'$.mrlc') in ('T_2','T_3','T_4','T_5','T_6')
 ) m
group by
  m.platform,
  m.recommend_position,
  m.country_number,
  m.country_name
;


--商品点击数	
INSERT OVERWRITE TABLE dw_proj.rosegal_pc_report_click_num_tmp
SELECT
count(cookie_id) as click_num,
m.platform,
m.recommend_position,
m.country_number,
m.country_name,
'${ADD_TIME}' as add_time
FROM
 (
    SELECT
      cookie_id,
      platform,
      get_json_object(sub_event_field,'$.mrlc')  as recommend_position,
      country_number,
      country_name 
    FROM
      ods.ods_pc_burial_log
    WHERE
      concat_ws('-', year, month, day) = '${ADD_TIME}'
      and behaviour_type = 'ic'
      and page_main_type = 'c'
      and page_module = 'mr'
      and sub_event_info = 'sku'
      and get_json_object(sub_event_field,'$.mrlc') 
      in ('T_2','T_3','T_4','T_5','T_6')
      and site='rosegal'
 ) m
group by
  m.platform,
  m.recommend_position,
  m.country_number,
  m.country_name

union all 

SELECT
count(cookie_id) as click_num,
m.platform,
m.recommend_position,
m.country_number,
m.country_name,
'${ADD_TIME}' as add_time
FROM
 (
    SELECT
      cookie_id,
      platform,
      get_json_object(sub_event_field,'$.mrlc')  as recommend_position,
      country_number,
      country_name 
    FROM
      ods.ods_pc_burial_log
    WHERE
      concat_ws('-', year, month, day) = '${ADD_TIME}'
      and behaviour_type = 'ic'
      and page_main_type = 'd'
      and page_module = 'mr'
      and sub_event_info = 'sku'
      and get_json_object(sub_event_field,'$.mrlc') 
      in ('T_2','T_3','T_4','T_5','T_6')
      and site='rosegal'
 ) m
group by
  m.platform,
  m.recommend_position,
  m.country_number,
  m.country_name

union all

SELECT
count(cookie_id) as click_num,
m.platform,
m.recommend_position,
m.country_number,
m.country_name,
'${ADD_TIME}' as add_time
FROM
 (
    SELECT
      cookie_id,
      platform,
      get_json_object(sub_event_field,'$.mrlc')  as recommend_position,
      country_number,
      country_name 
    FROM
      ods.ods_pc_burial_log
    WHERE
      concat_ws('-', year, month, day) = '${ADD_TIME}'
      and behaviour_type = 'ic'
      and page_sub_type = 'b02'
      and page_module = 'mr'
      and sub_event_info = 'sku'
      and get_json_object(sub_event_field,'$.mrlc') 
      in ('T_2','T_3','T_4','T_5','T_6')
      and site='rosegal'
 ) m
group by
  m.platform,
  m.recommend_position,
  m.country_number,
  m.country_name
;

--点击UV
INSERT OVERWRITE TABLE dw_proj.rosegal_pc_report_click_uv_tmp
SELECT
count(distinct cookie_id) as click_uv,
m.platform,
m.recommend_position,
m.country_number,
m.country_name,
'${ADD_TIME}' as add_time
FROM
 (
    SELECT
      cookie_id,
      platform,
      get_json_object(sub_event_field,'$.mrlc')  as recommend_position,
      country_number,
      country_name
    FROM
      ods.ods_pc_burial_log
    WHERE
      concat_ws('-', year, month, day) = '${ADD_TIME}'
      and behaviour_type = 'ic'
      and page_main_type = 'c'
      and page_module = 'mr'
      and sub_event_info = 'sku'
      and get_json_object(sub_event_field,'$.mrlc') 
      in ('T_2','T_3','T_4','T_5','T_6')
      and site='rosegal'
 ) m
group by
  m.platform,
  m.recommend_position,
  m.country_number,
  m.country_name

union all

SELECT
count(distinct cookie_id) as click_uv,
m.platform,
m.recommend_position,
m.country_number,
m.country_name,
'${ADD_TIME}' as add_time
FROM
 (
    SELECT
      cookie_id,
      platform,
      get_json_object(sub_event_field,'$.mrlc')  as recommend_position,
      country_number,
      country_name
    FROM
      ods.ods_pc_burial_log
    WHERE
      concat_ws('-', year, month, day) = '${ADD_TIME}'
      and behaviour_type = 'ic'
      and page_main_type = 'd'
      and page_module = 'mr'
      and sub_event_info = 'sku'
      and get_json_object(sub_event_field,'$.mrlc') 
      in ('T_2','T_3','T_4','T_5','T_6')
      and site='rosegal'
 ) m
group by
  m.platform,
  m.recommend_position,
  m.country_number,
  m.country_name

union all

SELECT
count(distinct cookie_id) as click_uv,
m.platform,
m.recommend_position,
m.country_number,
m.country_name,
'${ADD_TIME}' as add_time
FROM
 (
    SELECT
      cookie_id,
      platform,
      get_json_object(sub_event_field,'$.mrlc')  as recommend_position,
      country_number,
      country_name
    FROM
      ods.ods_pc_burial_log
    WHERE
      concat_ws('-', year, month, day) = '${ADD_TIME}'
      and behaviour_type = 'ic'
      and page_sub_type = 'b02'
      and page_module = 'mr'
      and sub_event_info = 'sku'
      and get_json_object(sub_event_field,'$.mrlc') 
      in ('T_2','T_3','T_4','T_5','T_6')
      and site='rosegal'
 ) m
group by
  m.platform,
  m.recommend_position,
  m.country_number,
  m.country_name
;



















--所有结果汇总
INSERT OVERWRITE TABLE dw_proj.rosegal_pc_recommend_position_report PARTITION (add_time = '${ADD_TIME}')
select
  NVL(a.pv,0),
  NVL(b.uv,0),
  c.exp_num,
  d.sku_uv,
  e.click_num,
  f.click_uv,
  e.click_num / c.exp_num * 100,
  f.click_uv / d.sku_uv * 100,
  g.cart_num,
  h.cart_uv,
  g.cart_num / c.exp_num * 100,
  h.cart_uv / d.sku_uv * 100,
  i.order_sku_num,
  j.order_uv,
  i.order_sku_num / c.exp_num * 100,
  j.order_uv / d.sku_uv * 100,
  k.gmv,
  l.purchase_num,
  m.pay_uv,
  n.pay_amount,
  l.purchase_num / c.exp_num * 100,
  m.pay_uv / d.sku_uv * 100,
  k.gmv / c.exp_num * 1000,
  o.collect_uv,
  p.collect_num,
  'pc',
  '商详页推荐T_3',
  '商详页推荐-related_recommendations',
  a.lang_code,
  a.country
from  
(select pv,add_time,lang_code,country from  dw_proj.rosegal_pc_report_pv_tmp where platform='pc' and recommend_position='related_recommendations' ) a 
left join 
(select uv,add_time,lang_code,country from  dw_proj.rosegal_pc_report_uv_tmp where platform='pc' and recommend_position='related_recommendations' ) b
on a.add_time=b.add_time and  a.lang_code=b.lang_code and a.country=b.country
left join
(select exp_num,add_time,lang_code,country from  dw_proj.rosegal_pc_report_exp_num_tmp where platform='pc' and recommend_position='T_3' ) c
on a.add_time=c.add_time and  a.lang_code=c.lang_code and a.country=c.country
left join
(select sku_uv,add_time,lang_code,country from  dw_proj.rosegal_pc_report_sku_uv_tmp where platform='pc' and recommend_position='T_3' ) d
on a.add_time=d.add_time and  a.lang_code=d.lang_code and a.country=d.country
left join
(select click_num,add_time,lang_code,country from  dw_proj.rosegal_pc_report_click_num_tmp where platform='pc' and recommend_position='T_3' ) e
on a.add_time=e.add_time and  a.lang_code=e.lang_code and a.country=e.country
left join
(select click_uv,add_time,lang_code,country from  dw_proj.rosegal_pc_report_click_uv_tmp where platform='pc' and recommend_position='T_3' ) f
on a.add_time=f.add_time and  a.lang_code=f.lang_code and a.country=f.country
left join
(select cart_num,add_time,lang_code,country from  dw_proj.rosegal_pc_report_cart_num_tmp where platform='pc' and recommend_position='mr_T_3' ) g
on a.add_time=g.add_time and  a.lang_code=g.lang_code and a.country=g.country
left join
(select cart_uv,add_time,lang_code,country from  dw_proj.rosegal_pc_report_cart_uv_tmp where platform='pc' and recommend_position='mr_T_3' ) h
on a.add_time=h.add_time and  a.lang_code=h.lang_code and a.country=h.country
left join
(select order_sku_num,add_time,lang_code,country from  dw_proj.rosegal_pc_report_order_sku_num_tmp where platform='pc' and recommend_position='mr_T_3' ) i
on a.add_time=i.add_time and  a.lang_code=i.lang_code and a.country=i.country
left join
(select order_uv,add_time,lang_code,country from  dw_proj.rosegal_pc_report_order_uv_tmp where platform='pc' and recommend_position='mr_T_3' ) j
on a.add_time=j.add_time and  a.lang_code=j.lang_code and a.country=j.country
left join
(select gmv,add_time,lang_code,country from  dw_proj.rosegal_pc_report_gmv_tmp where platform='pc' and recommend_position='mr_T_3' ) k
on a.add_time=k.add_time and  a.lang_code=k.lang_code and a.country=k.country
left join
(select purchase_num,add_time,lang_code,country from  dw_proj.rosegal_pc_report_purchase_num_tmp where platform='pc' and recommend_position='mr_T_3' ) l
on a.add_time=l.add_time and  a.lang_code=l.lang_code and a.country=l.country
left join
(select pay_uv,add_time,lang_code,country from  dw_proj.rosegal_pc_report_pay_uv_tmp where platform='pc' and recommend_position='mr_T_3' ) m
on a.add_time=m.add_time and  a.lang_code=m.lang_code and a.country=m.country
left join
(select pay_amount,add_time,lang_code,country from  dw_proj.rosegal_pc_report_pay_amount_tmp where platform='pc' and recommend_position='mr_T_3' ) n
on a.add_time=n.add_time and  a.lang_code=n.lang_code and a.country=n.country
left join
(select collect_uv,add_time,lang_code,country from  dw_proj.rosegal_pc_report_collect_uv_tmp where platform='pc' and recommend_position='mr_T_3' ) o
on a.add_time=o.add_time and  a.lang_code=o.lang_code and a.country=o.country
left join
(select collect_num,add_time,lang_code,country from  dw_proj.rosegal_pc_report_collect_num_tmp where platform='pc' and recommend_position='mr_T_3' ) p
on a.add_time=p.add_time and  a.lang_code=p.lang_code and a.country=p.country

union all

select
  NVL(a.pv,0),
  NVL(b.uv,0),
  c.exp_num,
  d.sku_uv,
  e.click_num,
  f.click_uv,
  e.click_num / c.exp_num * 100,
  f.click_uv / d.sku_uv * 100,
  g.cart_num,
  h.cart_uv,
  g.cart_num / c.exp_num * 100,
  h.cart_uv / d.sku_uv * 100,
  i.order_sku_num,
  j.order_uv,
  i.order_sku_num / c.exp_num * 100,
  j.order_uv / d.sku_uv * 100,
  k.gmv,
  l.purchase_num,
  m.pay_uv,
  n.pay_amount,
  l.purchase_num / c.exp_num * 100,
  m.pay_uv / d.sku_uv * 100,
  k.gmv / c.exp_num * 1000,
  o.collect_uv,
  p.collect_num,
  'pc',
  '商详页推荐T_4',
  '商详页推荐-customers_also_viewed',
  a.lang_code,
  a.country
from  
(select pv,add_time,lang_code,country from  dw_proj.rosegal_pc_report_pv_tmp where platform='pc' and recommend_position='customers_also_viewed' ) a 
left join 
(select uv,add_time,lang_code,country from  dw_proj.rosegal_pc_report_uv_tmp where platform='pc' and recommend_position='customers_also_viewed' ) b
on a.add_time=b.add_time and  a.lang_code=b.lang_code and a.country=b.country
left join
(select exp_num,add_time,lang_code,country from  dw_proj.rosegal_pc_report_exp_num_tmp where platform='pc' and recommend_position='T_4' ) c
on a.add_time=c.add_time and  a.lang_code=c.lang_code and a.country=c.country
left join
(select sku_uv,add_time,lang_code,country from  dw_proj.rosegal_pc_report_sku_uv_tmp where platform='pc' and recommend_position='T_4' ) d
on a.add_time=d.add_time and  a.lang_code=d.lang_code and a.country=d.country
left join
(select click_num,add_time,lang_code,country from  dw_proj.rosegal_pc_report_click_num_tmp where platform='pc' and recommend_position='T_4' ) e
on a.add_time=e.add_time and  a.lang_code=e.lang_code and a.country=e.country
left join
(select click_uv,add_time,lang_code,country from  dw_proj.rosegal_pc_report_click_uv_tmp where platform='pc' and recommend_position='T_4' ) f
on a.add_time=f.add_time and  a.lang_code=f.lang_code and a.country=f.country
left join
(select cart_num,add_time,lang_code,country from  dw_proj.rosegal_pc_report_cart_num_tmp where platform='pc' and recommend_position='mr_T_4' ) g
on a.add_time=g.add_time and  a.lang_code=g.lang_code and a.country=g.country
left join
(select cart_uv,add_time,lang_code,country from  dw_proj.rosegal_pc_report_cart_uv_tmp where platform='pc' and recommend_position='mr_T_4' ) h
on a.add_time=h.add_time and  a.lang_code=h.lang_code and a.country=h.country
left join
(select order_sku_num,add_time,lang_code,country from  dw_proj.rosegal_pc_report_order_sku_num_tmp where platform='pc' and recommend_position='mr_T_4' ) i
on a.add_time=i.add_time and  a.lang_code=i.lang_code and a.country=i.country
left join
(select order_uv,add_time,lang_code,country from  dw_proj.rosegal_pc_report_order_uv_tmp where platform='pc' and recommend_position='mr_T_4' ) j
on a.add_time=j.add_time and  a.lang_code=j.lang_code and a.country=j.country
left join
(select gmv,add_time,lang_code,country from  dw_proj.rosegal_pc_report_gmv_tmp where platform='pc' and recommend_position='mr_T_4' ) k
on a.add_time=k.add_time and  a.lang_code=k.lang_code and a.country=k.country
left join
(select purchase_num,add_time,lang_code,country from  dw_proj.rosegal_pc_report_purchase_num_tmp where platform='pc' and recommend_position='mr_T_4' ) l
on a.add_time=l.add_time and  a.lang_code=l.lang_code and a.country=l.country
left join
(select pay_uv,add_time,lang_code,country from  dw_proj.rosegal_pc_report_pay_uv_tmp where platform='pc' and recommend_position='mr_T_4' ) m
on a.add_time=m.add_time and  a.lang_code=m.lang_code and a.country=m.country
left join
(select pay_amount,add_time,lang_code,country from  dw_proj.rosegal_pc_report_pay_amount_tmp where platform='pc' and recommend_position='mr_T_4' ) n
on a.add_time=n.add_time and  a.lang_code=n.lang_code and a.country=n.country
left join
(select collect_uv,add_time,lang_code,country from  dw_proj.rosegal_pc_report_collect_uv_tmp where platform='pc' and recommend_position='mr_T_4' ) o
on a.add_time=o.add_time and  a.lang_code=o.lang_code and a.country=o.country
left join
(select collect_num,add_time,lang_code,country from  dw_proj.rosegal_pc_report_collect_num_tmp where platform='pc' and recommend_position='mr_T_4' ) p
on a.add_time=p.add_time and  a.lang_code=p.lang_code and a.country=p.country

union all

select
  NVL(a.pv,0),
  NVL(b.uv,0),
  c.exp_num,
  d.sku_uv,
  e.click_num,
  f.click_uv,
  e.click_num / c.exp_num * 100,
  f.click_uv / d.sku_uv * 100,
  g.cart_num,
  h.cart_uv,
  g.cart_num / c.exp_num * 100,
  h.cart_uv / d.sku_uv * 100,
  i.order_sku_num,
  j.order_uv,
  i.order_sku_num / c.exp_num * 100,
  j.order_uv / d.sku_uv * 100,
  k.gmv,
  l.purchase_num,
  m.pay_uv,
  n.pay_amount,
  l.purchase_num / c.exp_num * 100,
  m.pay_uv / d.sku_uv * 100,
  k.gmv / c.exp_num * 1000,
  o.collect_uv,
  p.collect_num,
  'pc',
  '空购物车页T_5',
  '空购物车页-featured_recommendations',
  a.lang_code,
  a.country
from  
(select pv,add_time,lang_code,country from  dw_proj.rosegal_pc_report_pv_tmp where platform='pc' and recommend_position='featured_recommendations' ) a 
left join 
(select uv,add_time,lang_code,country from  dw_proj.rosegal_pc_report_uv_tmp where platform='pc' and recommend_position='featured_recommendations' ) b
on a.add_time=b.add_time and  a.lang_code=b.lang_code and a.country=b.country
left join
(select exp_num,add_time,lang_code,country from  dw_proj.rosegal_pc_report_exp_num_tmp where platform='pc' and recommend_position='T_5' ) c
on a.add_time=c.add_time and  a.lang_code=c.lang_code and a.country=c.country
left join
(select sku_uv,add_time,lang_code,country from  dw_proj.rosegal_pc_report_sku_uv_tmp where platform='pc' and recommend_position='T_5' ) d
on a.add_time=d.add_time and  a.lang_code=d.lang_code and a.country=d.country
left join
(select click_num,add_time,lang_code,country from  dw_proj.rosegal_pc_report_click_num_tmp where platform='pc' and recommend_position='T_5' ) e
on a.add_time=e.add_time and  a.lang_code=e.lang_code and a.country=e.country
left join
(select click_uv,add_time,lang_code,country from  dw_proj.rosegal_pc_report_click_uv_tmp where platform='pc' and recommend_position='T_5' ) f
on a.add_time=f.add_time and  a.lang_code=f.lang_code and a.country=f.country
left join
(select cart_num,add_time,lang_code,country from  dw_proj.rosegal_pc_report_cart_num_tmp where platform='pc' and recommend_position='mr_T_5' ) g
on a.add_time=g.add_time and  a.lang_code=g.lang_code and a.country=g.country
left join
(select cart_uv,add_time,lang_code,country from  dw_proj.rosegal_pc_report_cart_uv_tmp where platform='pc' and recommend_position='mr_T_5' ) h
on a.add_time=h.add_time and  a.lang_code=h.lang_code and a.country=h.country
left join
(select order_sku_num,add_time,lang_code,country from  dw_proj.rosegal_pc_report_order_sku_num_tmp where platform='pc' and recommend_position='mr_T_5' ) i
on a.add_time=i.add_time and  a.lang_code=i.lang_code and a.country=i.country
left join
(select order_uv,add_time,lang_code,country from  dw_proj.rosegal_pc_report_order_uv_tmp where platform='pc' and recommend_position='mr_T_5' ) j
on a.add_time=j.add_time and  a.lang_code=j.lang_code and a.country=j.country
left join
(select gmv,add_time,lang_code,country from  dw_proj.rosegal_pc_report_gmv_tmp where platform='pc' and recommend_position='mr_T_5' ) k
on a.add_time=k.add_time and  a.lang_code=k.lang_code and a.country=k.country
left join
(select purchase_num,add_time,lang_code,country from  dw_proj.rosegal_pc_report_purchase_num_tmp where platform='pc' and recommend_position='mr_T_5' ) l
on a.add_time=l.add_time and  a.lang_code=l.lang_code and a.country=l.country
left join
(select pay_uv,add_time,lang_code,country from  dw_proj.rosegal_pc_report_pay_uv_tmp where platform='pc' and recommend_position='mr_T_5' ) m
on a.add_time=m.add_time and  a.lang_code=m.lang_code and a.country=m.country
left join
(select pay_amount,add_time,lang_code,country from  dw_proj.rosegal_pc_report_pay_amount_tmp where platform='pc' and recommend_position='mr_T_5' ) n
on a.add_time=n.add_time and  a.lang_code=n.lang_code and a.country=n.country
left join
(select collect_uv,add_time,lang_code,country from  dw_proj.rosegal_pc_report_collect_uv_tmp where platform='pc' and recommend_position='mr_T_5' ) o
on a.add_time=o.add_time and  a.lang_code=o.lang_code and a.country=o.country
left join
(select collect_num,add_time,lang_code,country from  dw_proj.rosegal_pc_report_collect_num_tmp where platform='pc' and recommend_position='mr_T_5' ) p
on a.add_time=p.add_time and  a.lang_code=p.lang_code and a.country=p.country

union all

select
  NVL(a.pv,0),
  NVL(b.uv,0),
  c.exp_num,
  d.sku_uv,
  e.click_num,
  f.click_uv,
  e.click_num / c.exp_num * 100,
  f.click_uv / d.sku_uv * 100,
  g.cart_num,
  h.cart_uv,
  g.cart_num / c.exp_num * 100,
  h.cart_uv / d.sku_uv * 100,
  i.order_sku_num,
  j.order_uv,
  i.order_sku_num / c.exp_num * 100,
  j.order_uv / d.sku_uv * 100,
  k.gmv,
  l.purchase_num,
  m.pay_uv,
  n.pay_amount,
  l.purchase_num / c.exp_num * 100,
  m.pay_uv / d.sku_uv * 100,
  k.gmv / c.exp_num * 1000,
  o.collect_uv,
  p.collect_num,
  'pc',
  '无搜索结果页T_6',
  '无搜索结果页-may_be_you_like',
  a.lang_code,
  a.country
from  
(select pv,add_time,lang_code,country from  dw_proj.rosegal_pc_report_pv_tmp where platform='pc' and recommend_position='may_be_you_like' ) a 
left join 
(select uv,add_time,lang_code,country from  dw_proj.rosegal_pc_report_uv_tmp where platform='pc' and recommend_position='may_be_you_like' ) b
on a.add_time=b.add_time and  a.lang_code=b.lang_code and a.country=b.country
left join
(select exp_num,add_time,lang_code,country from  dw_proj.rosegal_pc_report_exp_num_tmp where platform='pc' and recommend_position='T_6' ) c
on a.add_time=c.add_time and  a.lang_code=c.lang_code and a.country=c.country
left join
(select sku_uv,add_time,lang_code,country from  dw_proj.rosegal_pc_report_sku_uv_tmp where platform='pc' and recommend_position='T_6' ) d
on a.add_time=d.add_time and  a.lang_code=d.lang_code and a.country=d.country
left join
(select click_num,add_time,lang_code,country from  dw_proj.rosegal_pc_report_click_num_tmp where platform='pc' and recommend_position='T_6' ) e
on a.add_time=e.add_time and  a.lang_code=e.lang_code and a.country=e.country
left join
(select click_uv,add_time,lang_code,country from  dw_proj.rosegal_pc_report_click_uv_tmp where platform='pc' and recommend_position='T_6' ) f
on a.add_time=f.add_time and  a.lang_code=f.lang_code and a.country=f.country
left join
(select cart_num,add_time,lang_code,country from  dw_proj.rosegal_pc_report_cart_num_tmp where platform='pc' and recommend_position='mr_T_6' ) g
on a.add_time=g.add_time and  a.lang_code=g.lang_code and a.country=g.country
left join
(select cart_uv,add_time,lang_code,country from  dw_proj.rosegal_pc_report_cart_uv_tmp where platform='pc' and recommend_position='mr_T_6' ) h
on a.add_time=h.add_time and  a.lang_code=h.lang_code and a.country=h.country
left join
(select order_sku_num,add_time,lang_code,country from  dw_proj.rosegal_pc_report_order_sku_num_tmp where platform='pc' and recommend_position='mr_T_6' ) i
on a.add_time=i.add_time and  a.lang_code=i.lang_code and a.country=i.country
left join
(select order_uv,add_time,lang_code,country from  dw_proj.rosegal_pc_report_order_uv_tmp where platform='pc' and recommend_position='mr_T_6' ) j
on a.add_time=j.add_time and  a.lang_code=j.lang_code and a.country=j.country
left join
(select gmv,add_time,lang_code,country from  dw_proj.rosegal_pc_report_gmv_tmp where platform='pc' and recommend_position='mr_T_6' ) k
on a.add_time=k.add_time and  a.lang_code=k.lang_code and a.country=k.country
left join
(select purchase_num,add_time,lang_code,country from  dw_proj.rosegal_pc_report_purchase_num_tmp where platform='pc' and recommend_position='mr_T_6' ) l
on a.add_time=l.add_time and  a.lang_code=l.lang_code and a.country=l.country
left join
(select pay_uv,add_time,lang_code,country from  dw_proj.rosegal_pc_report_pay_uv_tmp where platform='pc' and recommend_position='mr_T_6' ) m
on a.add_time=m.add_time and  a.lang_code=m.lang_code and a.country=m.country
left join
(select pay_amount,add_time,lang_code,country from  dw_proj.rosegal_pc_report_pay_amount_tmp where platform='pc' and recommend_position='mr_T_6' ) n
on a.add_time=n.add_time and  a.lang_code=n.lang_code and a.country=n.country
left join
(select collect_uv,add_time,lang_code,country from  dw_proj.rosegal_pc_report_collect_uv_tmp where platform='pc' and recommend_position='mr_T_6' ) o
on a.add_time=o.add_time and  a.lang_code=o.lang_code and a.country=o.country
left join
(select collect_num,add_time,lang_code,country from  dw_proj.rosegal_pc_report_collect_num_tmp where platform='pc' and recommend_position='mr_T_6' ) p
on a.add_time=p.add_time and  a.lang_code=p.lang_code and a.country=p.country

;



--结果导出到mysql
INSERT OVERWRITE TABLE dw_proj.rosegal_pc_recommend_position_report_exp
select
 *
from 
  dw_proj.rosegal_pc_recommend_position_report
where 
  add_time = '${ADD_TIME}'
  and platform = 'pc' 
-- union all 
-- select
--  *
-- from 
--   dw_proj.rosegal_pc_recommend_position_report
-- where 
--   add_time = '${ADD_TIME}'
--   and platform = 'pc' 
--   and recommend_position ='空购物车页-featured_recommendations'
;