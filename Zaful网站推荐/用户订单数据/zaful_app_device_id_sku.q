
--@author wuchao
--@date 2019年1月15日 
--@desc  Zaful App_sku


SET mapred.job.name=zaful_app_device_id_sku;
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

create table if not exists dw_proj.zaful_app_device_id_sku_time_category_wuc(
 appsflyer_device_id                        STRING    comment 'device_id',
 event_name              STRING       comment '事件名',
 skus_time            STRING       comment 'sku#time',
 add_times                 INT       comment '时间戳',
 date                       int       comment '时间',       
 platform                   STRING    comment '平台',
 country_code               STRING    comment '国家代码'
 )
 PARTITIONED BY (add_time STRING)
 ROW FORMAT DELIMITED FIELDS TERMINATED BY '\u0001' LINES TERMINATED BY '\n' STORED AS TEXTFILE
;
--device_id_sku 列表页
insert OVERWRITE table dw_proj.zaful_app_device_id_sku_time_category_wuc  PARTITION (add_time='${ADD_TIME}')
select c.appsflyer_device_id,c.event_name,concat_ws(',',collect_set(c.sku_time)) as skus_time,
UNIX_TIMESTAMP('${ADD_TIME}','yyyyMMdd') as add_times,
from_unixtime(UNIX_TIMESTAMP('${ADD_TIME}','yyyyMMdd'),'yyyyMMdd') as date,
c.platform,c.country_code
from (
    select b.appsflyer_device_id,b.event_name,concat(b.sku,'#',b.event_time) as sku_time,b.platform,b.country_code
    from (
        select a.appsflyer_device_id,a.event_name,a.event_time,a.skus,sku,a.platform,a.country_code
        from (
            select 
            appsflyer_device_id,event_name,event_time,
            get_json_object(event_value,'$.af_content_id') as skus,
            platform,country_code
            from  
            ods.ods_app_burial_log 
            WHERE
            concat(year,month,day) = '${ADD_TIME}'
            and site='zaful'
            AND (event_name='af_add_to_bag' OR event_name='af_view_product'  OR event_name='af_impression' )
             AND  get_json_object(event_value, '$.af_inner_mediasource') like 'category_%'
            --and length(get_json_object(event_value,'$.af_content_id'))<20
            --and appsflyer_device_id='1533491853806-6941657'
            --limit 10
            ) a
        LATERAL VIEW explode(split(a.skus,',')) zqms as sku
        ) b
    group by b.appsflyer_device_id,b.event_name,b.event_time,b.sku,b.platform,b.country_code
    ) c 
group by c.appsflyer_device_id,c.event_name,c.platform,c.country_code
;

create table if not exists dw_proj.zaful_app_device_id_sku_time_all_wuc(
 appsflyer_device_id                        STRING    comment 'device_id',
 event_name              STRING       comment '事件名',
 skus_time            STRING       comment 'sku#time',
 add_times                 INT       comment '时间戳',
 date                       int       comment '时间',       
 platform                   STRING    comment '平台',
 country_code               STRING    comment '国家代码'
 )
 PARTITIONED BY (add_time STRING)
 ROW FORMAT DELIMITED FIELDS TERMINATED BY '\u0001' LINES TERMINATED BY '\n' STORED AS TEXTFILE
;
--device_id_sku全局
insert OVERWRITE table dw_proj.zaful_app_device_id_sku_time_all_wuc  PARTITION (add_time='${ADD_TIME}')
select c.appsflyer_device_id,c.event_name,concat_ws(',',collect_set(c.sku_time)) as skus_time,
UNIX_TIMESTAMP('${ADD_TIME}','yyyyMMdd') as add_times,
from_unixtime(UNIX_TIMESTAMP('${ADD_TIME}','yyyyMMdd'),'yyyyMMdd') as date,
c.platform,c.country_code
from (
    select b.appsflyer_device_id,b.event_name,concat(b.sku,'#',b.event_time) as sku_time,b.platform,b.country_code
    from (
        select a.appsflyer_device_id,a.event_name,a.event_time,a.skus,sku,a.platform,a.country_code
        from (
            select 
            appsflyer_device_id,event_name,event_time,
            get_json_object(event_value,'$.af_content_id') as skus,
            platform,country_code
            from  
            ods.ods_app_burial_log 
            WHERE
            concat(year,month,day) = '${ADD_TIME}'
            and site='zaful'
            AND (event_name='af_add_to_bag' OR event_name='af_view_product'  OR event_name='af_impression' )
            AND  get_json_object(event_value, '$.af_inner_mediasource') !='unknow mediasource'
            --and length(get_json_object(event_value,'$.af_content_id'))<20
            --and appsflyer_device_id='1533491853806-6941657'
            --limit 10
            ) a
        LATERAL VIEW explode(split(a.skus,',')) zqms as sku
        ) b
    group by b.appsflyer_device_id,b.event_name,b.event_time,b.sku,b.platform,b.country_code
    ) c 
group by c.appsflyer_device_id,c.event_name,c.platform,c.country_code
;


--下单device_id,sku
create table if not exists dw_proj.zaful_app_device_id_order_sku_time_all_wuc(
 appsflyer_device_id                        STRING    comment 'device_id',
 user_id              STRING       comment '',
 skus_time            STRING       comment 'sku#time',
 add_times                 INT       comment '时间戳',
 date                       int       comment '时间',       
 platform                   STRING    comment '平台',
 country_code               STRING    comment '国家代码'
 )
 PARTITIONED BY (add_time STRING)
 ROW FORMAT DELIMITED FIELDS TERMINATED BY '\u0001' LINES TERMINATED BY '\n' STORED AS TEXTFILE
;

insert OVERWRITE table dw_proj.zaful_app_device_id_order_sku_time_all_wuc  PARTITION (add_time='${ADD_TIME}')
 select a.appsflyer_device_id,a.user_id,concat_ws(',',collect_set(a.sku_time)),
 UNIX_TIMESTAMP('${ADD_TIME}','yyyyMMdd') as add_times,
from_unixtime(UNIX_TIMESTAMP('${ADD_TIME}','yyyyMMdd'),'yyyyMMdd') as date,
 a.platform,a.country_code
 from 
 (
    select k.appsflyer_device_id,l.user_id,concat(l.sku,'#',l.add_time) as sku_time,l.platform,l.country_code
    from 
    (
        select 
        t.appsflyer_device_id,t.customer_user_id as user_id
        from 
        (
            select 
            a.appsflyer_device_id,b.customer_user_id
            from 
                (select 
                appsflyer_device_id
                from  
                ods.ods_app_burial_log 
                WHERE
                concat(year,month,day) = '${ADD_TIME}'
                and site='zaful'
                AND  get_json_object(event_value, '$.af_inner_mediasource') !='unknow mediasource'
                ) a
                left join
                (select * from dw_zaful_recommend.zaful_app_u_map) b
                on a.appsflyer_device_id=b.appsflyer_device_id
        ) t
        
        group by t.appsflyer_device_id,t.customer_user_id
    ) k
    inner join 
    (
        select
        m.user_id as user_id,m.sku,m.add_time,m.platform,m.country_code
        from (
        SELECT
        p.goods_sn as sku,
        x.user_id as user_id,
        x.add_time as add_time,
        case when x.order_sn LIKE 'UA%'  OR   x.order_sn   LIKE 'UUA%' THEN 'ios'  
        WHEN x.order_sn LIKE 'UB%'  OR   x.order_sn   LIKE 'UUB%' THEN 'android'  end
        as platform,
        x.region_code as country_code
        FROM
        (
            select 
            f.order_id as order_id,
            f.add_time as add_time,
            f.user_id as user_id,
            f.order_sn as order_sn,
            g.region_code as region_code
            from 
            (
            SELECT
            order_id,
            add_time,
            user_id,
            order_sn,
            country
            FROM
            ods.ods_m_zaful_eload_order_info
            WHERE
            from_unixtime(add_time+8*3600, 'yyyyMMdd') = '${ADD_TIME}'
            AND (order_sn LIKE 'UA%'  OR   order_sn   LIKE 'UUA%' 
                OR	order_sn LIKE 'UB%'  OR   order_sn   LIKE 'UUB%' )
            and dt='${ADD_TIME}'
            --and dt='20190331'
            ) f
            left join  stg.zaful_eload_region g 
            on f.country=g.region_id
    
        ) x
        inner JOIN
        (
        SELECT goods_sn,order_id
        from
        ods.ods_m_zaful_eload_order_goods 
        where 
        dt= '${ADD_TIME}'
         --dt='20190331'
         ) p
        ON x.order_id = p.order_id
        ) as m
        
        GROUP BY m.sku,m.platform,m.country_code,m.user_id,m.add_time 
    ) l
    on k.user_id=l.user_id
 ) a 
 where a.user_id is not null or a.user_id !=''
 group by a.appsflyer_device_id,a.user_id,a.platform,a.country_code
 ;


 create table if not exists dw_proj.zaful_app_device_id_order_sku_time_userid_all_wuc(
 appsflyer_device_id                        STRING    comment 'device_id',
 user_id              STRING       comment '',
 skus_time            STRING       comment 'sku#time',
 add_times                 INT       comment '时间戳',
 date                       int       comment '时间',       
 platform                   STRING    comment '平台',
 country_code               STRING    comment '国家代码'
 )
 PARTITIONED BY (add_time STRING)
 ROW FORMAT DELIMITED FIELDS TERMINATED BY '\u0001' LINES TERMINATED BY '\n' STORED AS TEXTFILE
;

insert OVERWRITE table dw_proj.zaful_app_device_id_order_sku_time_userid_all_wuc  PARTITION (add_time='${ADD_TIME}')
select 
appsflyer_device_id, user_id,skus_time,add_times,date,platform,country_code
 from (
        select 
            appsflyer_device_id, user_id,skus_time,add_times,date,platform,country_code,
            row_number() over ( partition by user_id order by rand()  ) num 
        from  
                dw_proj.zaful_app_device_id_order_sku_time_all_wuc
        where add_time='${ADD_TIME}'
      ) last 
  where last.num = 1 ;


  --下单device_id,sku 加order_status
create table if not exists dw_proj.zaful_app_device_id_order_sku_time_all_order_status(
 appsflyer_device_id                        STRING    comment 'device_id',
 user_id              STRING       comment '',
 skus_time            STRING       comment 'sku#time',
 add_times                 INT       comment '时间戳',
 date                       int       comment '时间',       
 platform                   STRING    comment '平台',
 country_code               STRING    comment '国家代码'
 )
 PARTITIONED BY (add_time STRING)
 ROW FORMAT DELIMITED FIELDS TERMINATED BY '\u0001' LINES TERMINATED BY '\n' STORED AS TEXTFILE
;

insert OVERWRITE table dw_proj.zaful_app_device_id_order_sku_time_all_order_status  PARTITION (add_time='${ADD_TIME}')
 select a.appsflyer_device_id,a.user_id,concat_ws(',',collect_set(a.sku_time)),
 UNIX_TIMESTAMP('${ADD_TIME}','yyyyMMdd') as add_times,
from_unixtime(UNIX_TIMESTAMP('${ADD_TIME}','yyyyMMdd'),'yyyyMMdd') as date,
 a.platform,a.country_code
 from 
 (
    select k.appsflyer_device_id,l.user_id,concat(l.sku,'#',l.add_time) as sku_time,l.platform,l.country_code
    from 
    (
        select 
        t.appsflyer_device_id,t.customer_user_id as user_id
        from 
        (
            select 
            a.appsflyer_device_id,b.customer_user_id
            from 
                (select 
                appsflyer_device_id
                from  
                ods.ods_app_burial_log 
                WHERE
                concat(year,month,day) = '${ADD_TIME}'
                and site='zaful'
                AND  get_json_object(event_value, '$.af_inner_mediasource') !='unknow mediasource'
                ) a
                left join
                (select * from dw_zaful_recommend.zaful_app_u_map) b
                on a.appsflyer_device_id=b.appsflyer_device_id
        ) t
        
        group by t.appsflyer_device_id,t.customer_user_id
    ) k
    inner join 
    (
        select
        m.user_id as user_id,m.sku,m.add_time,m.platform,m.country_code
        from (
        SELECT
        p.goods_sn as sku,
        x.user_id as user_id,
        x.add_time as add_time,
        case when x.order_sn LIKE 'UA%'  OR   x.order_sn   LIKE 'UUA%' THEN 'ios'  
        WHEN x.order_sn LIKE 'UB%'  OR   x.order_sn   LIKE 'UUB%' THEN 'android'  end
        as platform,
        x.region_code as country_code
        FROM
        (
            select 
            f.order_id as order_id,
            f.add_time as add_time,
            f.user_id as user_id,
            f.order_sn as order_sn,
            g.region_code as region_code
            from 
            (
            SELECT
            order_id,
            add_time,
            user_id,
            order_sn,
            country
            FROM
            ods.ods_m_zaful_eload_order_info
            WHERE
            from_unixtime(add_time+8*3600, 'yyyyMMdd') = '${ADD_TIME}'
            AND (order_sn LIKE 'UA%'  OR   order_sn   LIKE 'UUA%' 
                OR	order_sn LIKE 'UB%'  OR   order_sn   LIKE 'UUB%' )
            and dt='${ADD_TIME}'
            --and dt='20190331'
            and order_status not in ('0','11','13')
            ) f
            left join  stg.zaful_eload_region g 
            on f.country=g.region_id
    
        ) x
        inner JOIN
        (
        SELECT goods_sn,order_id
        from
        ods.ods_m_zaful_eload_order_goods 
        where 
        dt= '${ADD_TIME}'
        -- dt='20190331'
         ) p
        ON x.order_id = p.order_id
        ) as m
        
        GROUP BY m.sku,m.platform,m.country_code,m.user_id,m.add_time 
    ) l
    on k.user_id=l.user_id
 ) a 
 where a.user_id is not null or a.user_id !=''
 group by a.appsflyer_device_id,a.user_id,a.platform,a.country_code
 ;


 create table if not exists dw_proj.zaful_app_device_id_order_sku_time_userid_all_order_status(
 appsflyer_device_id                        STRING    comment 'device_id',
 user_id              STRING       comment '',
 skus_time            STRING       comment 'sku#time',
 add_times                 INT       comment '时间戳',
 date                       int       comment '时间',       
 platform                   STRING    comment '平台',
 country_code               STRING    comment '国家代码'
 )
 PARTITIONED BY (add_time STRING)
 ROW FORMAT DELIMITED FIELDS TERMINATED BY '\u0001' LINES TERMINATED BY '\n' STORED AS TEXTFILE
;

insert OVERWRITE table dw_proj.zaful_app_device_id_order_sku_time_userid_all_order_status  PARTITION (add_time='${ADD_TIME}')
select 
appsflyer_device_id, user_id,skus_time,add_times,date,platform,country_code
 from (
        select 
            appsflyer_device_id, user_id,skus_time,add_times,date,platform,country_code,
            row_number() over ( partition by user_id order by rand()  ) num 
        from  
                dw_proj.zaful_app_device_id_order_sku_time_all_order_status
        where add_time='${ADD_TIME}'
      ) last 
  where last.num = 1 ;