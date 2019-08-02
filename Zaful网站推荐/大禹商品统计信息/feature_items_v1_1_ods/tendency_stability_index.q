
SET mapred.job.name=feature_items_v2_2_ods;
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
set hive.support.concurrency=false;    

  create table if not exists dw_proj.feature_items_v2_2_ods_order_time(
 sku                  STRING       comment '',
 platform             STRING       comment '',
 order_sn              STRING       comment '',
 add_time                  STRING       comment '',
 date                  STRING       comment ''
 )
 ROW FORMAT DELIMITED FIELDS TERMINATED BY '\u0001' LINES TERMINATED BY '\n' STORED AS TEXTFILE
 ;                  

--取订单信息
insert overwrite table dw_proj.feature_items_v2_2_ods_order_time    
select
m.sku,m.platform,m.order_sn,m.add_time,m.date
from (
SELECT
  p.goods_sn as sku,
  x.order_sn as order_sn,
  x.add_time as add_time,
  x.date as date,
  case when x.order_sn LIKE 'UU1%' OR   x.order_sn   LIKE 'U1%'  THEN 'pc'
       WHEN x.order_sn LIKE 'UL%'  OR   x.order_sn   LIKE 'UM%'  THEN 'm'
       WHEN x.order_sn LIKE 'UA%'  OR   x.order_sn   LIKE 'UUA%' THEN 'ios'
       WHEN x.order_sn LIKE 'UB%'  OR   x.order_sn   LIKE 'UUB%' THEN 'android'
  else 'null' end as platform
FROM
  (
    select 
    f.order_id as order_id,
    f.add_time as add_time,
    from_unixtime(add_time+8*3600, 'yyyyMMdd') as date,
    f.order_sn as order_sn
    from 
    (
    SELECT
      order_id,
      add_time,
      order_sn,
      country
    FROM
      ods.ods_m_zaful_eload_order_info
    WHERE
      from_unixtime(add_time+8*3600, 'yyyy-MM-dd') between date_sub('${DATE_W}',2) and '${DATE_W}'
      and (order_sn LIKE 'UU1%' OR   order_sn   LIKE 'U1%'  OR 
      order_sn LIKE 'UL%'  OR   order_sn   LIKE 'UM%'  OR 
      order_sn LIKE 'UA%'  OR   order_sn   LIKE 'UUA%' OR 
      order_sn LIKE 'UB%'  OR   order_sn   LIKE 'UUB%' )
       and dt='${DATE}'
    ) f
  ) x
  inner JOIN
  (
  SELECT goods_sn,order_id
  from
   ods.ods_m_zaful_eload_order_goods where dt= '${DATE}') p
   ON x.order_id = p.order_id
) as m  
GROUP BY m.sku,m.platform,m.order_sn,m.add_time,m.date    
;   

--计算热度指数
create table if not exists dw_proj.feature_items_v2_2_ods_order_time_count(
 sku                  STRING       comment '',
 platform             STRING       comment '',
 time_order_count                  double       comment '热度指数',
 date                  STRING       comment ''
 )
 ROW FORMAT DELIMITED FIELDS TERMINATED BY '\u0001' LINES TERMINATED BY '\n' STORED AS TEXTFILE
 ;    

insert overwrite table dw_proj.feature_items_v2_2_ods_order_time_count 
select 
m.sku,m.platform,sum(m.time)/sum(m.order_count),'${DATE}'
from
(
    select a.sku,a.platform,a.order_count,(b.add_time-c.add_time)/60 as time,a.date 
    from
        (
            select sku,platform,count(order_sn) as order_count,date
            from dw_proj.feature_items_v2_2_ods_order_time
            group by sku,platform,date
        ) a
        left join
        (
            select 
            x.sku,x.platform,x.add_time,x.date
            from
            (
                select sku,platform,add_time,date,row_number() over ( partition by sku,platform,date order by add_time desc  ) num 
                from dw_proj.feature_items_v2_2_ods_order_time
                group by sku,platform,add_time,date
            ) x
            where x.num=1
        ) b
        on a.sku=b.sku and a.platform=b.platform and a.date=b.date
        left join
        (
            select 
            x.sku,x.platform,x.add_time,x.date
            from
            (
                select sku,platform,add_time,date,row_number() over ( partition by sku,platform,date order by add_time asc  ) num 
                from dw_proj.feature_items_v2_2_ods_order_time
                group by sku,platform,add_time,date
            ) x
            where x.num=1
        ) c
        on a.sku=c.sku and a.platform=c.platform and a.date=c.date
) m
group by m.sku,m.platform
;




--统一计算，拼接热度指数
CREATE TABLE IF NOT EXISTS dw_zaful_recommend.feature_items_v2_2_ods_info_new_label (
    item_id String COMMENT '商品sku编号',
    ctr_rising_tendency         double COMMENT '前14天CTR增长趋势',
    cvr_rising_tendency         double COMMENT '前14天CVR增长趋势',
    ctr_stability_index         double COMMENT '前14天CTR稳定性指数',
    cvr_stability_index         double COMMENT '前14天CVR稳定性指数',
    heat_index                  double COMMENT '前3天的热度指数',
    platform                    String COMMENT '平台'
    )
PARTITIONED BY (year String, month String, day String)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' LINES TERMINATED BY '\n' 
STORED AS TEXTFILE;

insert overwrite table dw_zaful_recommend.feature_items_v2_2_ods_info_new_label partition (year='${YEAR}',month='${MONTH}',day='${DAY}')  
select
coalesce(a1.item_id,c.sku,'') as item_id,
a1.ctr_rising_tendency,
a1.cvr_rising_tendency,
a1.ctr_stability_index,
a1.cvr_stability_index,
c.time_order_count,
coalesce(a1.platform,c.platform,'') as platform
from
(
    select 
    coalesce(a.item_id,b.item_id,'') as item_id,
    a.ctr/b.avg_ctr as ctr_rising_tendency,
    a.cvr/b.avg_cvr as cvr_rising_tendency,
    b.stddev_ctr as ctr_stability_index,
    b.stddev_cvr as cvr_stability_index,
    coalesce(a.platform,b.platform,'') as platform
    from
        (
            select item_id,platform,ctr,cvr from dw_zaful_recommend.feature_items_v2_2_ods_info 
            where 
            concat(year,'-',month,'-',day) = '${DATE_W}'
        ) a
        left join 
        (
            select item_id,platform,avg(ctr) as avg_ctr,avg(cvr) as avg_cvr,stddev(ctr) as stddev_ctr,stddev(cvr) as stddev_cvr  from dw_zaful_recommend.feature_items_v2_2_ods_info 
            where 
            concat(year,'-',month,'-',day) between date_sub('${DATE_W}',13) and '${DATE_W}'
            group by item_id,platform
        ) b
        on a.item_id=b.item_id and a.platform=b.platform
) a1
full join
(
    select sku,platform,time_order_count,date from dw_proj.feature_items_v2_2_ods_order_time_count
) c
on a1.item_id=c.sku and a1.platform=c.platform
;