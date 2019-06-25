set mapred.job.queue.name = root.ai.offline;
--商品点击数据(天)
insert overwrite table zaful_recommend.goods_click_day_country
select 
    a.*,
    a.geoip_city_country_code as country_code 
from (
    select 
        *,
        get_json_object(glb_skuinfo, '$.sku') as goods_sn 
    from 
        stg.zf_pc_event_info 
    where 
        year=${YEAR} 
        and month=${MONTH} 
        and day=${DAY}
    ) a;

set mapred.job.queue.name = root.ai.offline;
--商品曝光数据(天)
insert overwrite table zaful_recommend.goods_exposure_day_country 
select 
    m.glb_plf,
    m.country_code,
    m.goods_sn,
    count(*) as number,
    count(distinct m.glb_od) as uv,
    count(*)/count(distinct m.glb_od) as aver 
from (
    select 
        a.glb_od,
        a.country_code,
        a.glb_plf,
        get_json_object(b.glb_ubcta_col,'$.sku') as goods_sn 
    from (
        select 
            glb_od,
            geoip_city_country_code as country_code,
            log_id,
            glb_plf,
            year,
            month,
            day 
        from 
            zaful_recommend.goods_click_day_country 
        where 
            glb_t = 'ie' 
            and glb_ubcta!=''
        ) a 
    join (
        select 
            log_id,
            glb_ubcta_col,
            year,
            month,
            day 
        from 
            stg.zf_pc_event_ubcta_info 
        where 
            year=${YEAR}
            and month=${MONTH} 
            and day=${DAY}
        ) b 
    on 
        a.log_id = b.log_id 
        and a.year=b.year 
        and a.month=b.month 
        and a.day=b.day
    ) m 
group by 
    m.glb_plf,
    m.country_code,
    m.goods_sn;

set mapred.job.queue.name = root.ai.offline;
--商品下单数据(天)
insert overwrite table zaful_recommend.goods_order_day_country 
select 
    x.glb_plf,
    x.country_code,
    x.goods_sn,
    sum(x.goods_number) as number,
    count(distinct x.user_id) as uv,
    sum(x.goods_number)/count(distinct x.user_id) as aver 
from (
    select 
        p.glb_plf,
        p.user_id,
        p.goods_sn,
        p.goods_number,
        q.country_code 
    from (
        select 
            'pc' as glb_plf,
            f.country,
            f.user_id,
            g.goods_sn,
            g.goods_number 
        from (
            select 
                distinct * 
            from (
                select 
                    country,
                    order_id,
                    user_id 
                from 
                    stg.zaful_eload_order_info 
                where 
                    order_status!=0 and order_status!=11 
                    and from_unixtime(add_time,'yyyyMMdd')=${DATE} 
                    and (order_sn like 'UU1%' or order_sn like 'U1%')
                ) a
            ) f 
        join 
            stg.zaful_eload_order_goods g 
        on 
            f.order_id = g.order_id 
    union all 
        select 
            'm' as glb_plf,
            m.country,
            m.user_id,
            n.goods_sn,
            n.goods_number 
        from (
            select 
                distinct * 
            from (
                select 
                    country,
                    order_id,
                    user_id 
                from 
                    stg.zaful_eload_order_info 
                where 
                    order_status!=0 and order_status!=11 
                    and from_unixtime(add_time,'yyyyMMdd')=${DATE} 
                    and (order_sn like 'UL%' or order_sn like 'UM%')
                ) b
            ) m 
        join 
            stg.zaful_eload_order_goods n 
        on 
            m.order_id = n.order_id
    ) p 
    join (
        select 
            distinct * 
        from (
            select 
                region_id,
                region_code as country_code 
            from 
                stg.zaful_eload_region
            ) d
        ) q 
    on 
        p.country=q.region_id
) x 
group by 
    x.glb_plf,
    x.country_code,
    x.goods_sn;


set mapred.job.queue.name = root.ai.offline;
--pc端整合
insert overwrite table zaful_recommend.item_info_pc_country11 
select 
    b.goods_sn as item_id,
    b.country_code as country,
    b.number as pv_cnt,
    b.uv as pv_uv,
    b.aver as pv_per_cnt,
    c.item_click_cnt_pc as ipv_cnt,
    c.item_click_uv_pc as ipv_uv,
    c.item_click_per_cnt_pc as ipv_per_cnt,
    d.item_cart_cnt_pc as bag_cnt,
    d.item_cart_uv_pc as bag_uv,
    d.item_cart_per_cnt_pc as bag_per_cnt,
    e.item_collected_cnt_pc as favorite_cnt,
    e.item_collected_uv_pc as favorite_uv,
    e.item_collected_per_cnt_pc as favorite_per_cnt,
    f.number as order_cnt,
    f.uv as order_uv,
    f.aver as order_per_cnt,
    (f.number/c.item_click_cnt_pc) as cvr, 
    (f.uv/c.item_click_uv_pc) as uv_cvr 
from (
    select 
        * 
    from 
        zaful_recommend.goods_exposure_day_country 
    where 
        glb_plf='pc'
    ) b 
left join (
    select 
        country_code,
        goods_sn,
        count(*) as item_click_cnt_pc,
        count(distinct glb_od) as item_click_uv_pc,
        count(*)/count(distinct glb_od) as item_click_per_cnt_pc 
    from 
        zaful_recommend.goods_click_day_country
    where 
        glb_t = 'ic' 
        and glb_x in ('sku','addtobag') 
        and glb_od!='' 
        and get_json_object(glb_skuinfo,'$.sku')!='' 
        and glb_plf='pc' 
    group by 
        country_code,
        goods_sn
    ) c 
on 
    b.goods_sn=c.goods_sn 
    and b.country_code=c.country_code 
left join (
    select 
        country_code,
        goods_sn,count(*) as item_cart_cnt_pc,
        count(distinct glb_od) as item_cart_uv_pc,
        count(*)/count(distinct glb_od) as item_cart_per_cnt_pc 
    from 
        zaful_recommend.goods_click_day_country
    where 
        glb_t = 'ic' 
        and glb_x = 'ADT' 
        and get_json_object(glb_skuinfo,'$.sku')!='' 
        and glb_plf='pc' 
    group by 
        country_code,
        goods_sn
    ) d 
on 
    b.goods_sn=d.goods_sn 
    and b.country_code=d.country_code 
left join (
    select 
        country_code,
        goods_sn,
        count(*) as item_collected_cnt_pc,
        count(distinct glb_od) as item_collected_uv_pc,
        count(*)/count(distinct glb_od) as item_collected_per_cnt_pc 
    from 
        zaful_recommend.goods_click_day_country 
    where 
        glb_t = 'ic' 
        and glb_x = 'ADF' 
        and glb_u!='' 
        and get_json_object(glb_skuinfo,'$.sku')!='' 
        and glb_plf='pc' 
    group by 
        country_code,
        goods_sn
    ) e 
on 
    b.goods_sn=e.goods_sn 
    and b.country_code=e.country_code 
left join (
    select 
        * 
    from 
        zaful_recommend.goods_order_day_country 
    where 
        glb_plf='pc'
    ) f 
on 
    b.goods_sn=f.goods_sn 
    and b.country_code=f.country_code
where b.number is not null;


set mapred.job.queue.name = root.ai.offline;
--m端整合
insert overwrite table zaful_recommend.item_info_m_country11 
select 
    b.goods_sn as item_id,
    b.country_code as country,
    b.number as pv_cnt,
    b.uv as pv_uv,
    b.aver as pv_per_cnt,
    c.item_click_cnt_m as ipv_cnt,
    c.item_click_uv_m as ipv_uv,
    c.item_click_per_cnt_m as ipv_per_cnt,
    d.item_cart_cnt_m as bag_cnt,
    d.item_cart_uv_m as bag_uv,
    d.item_cart_per_cnt_m as bag_per_cnt,
    e.item_collected_cnt_m as favorite_cnt,
    e.item_collected_uv_m as favorite_uv,
    e.item_collected_per_cnt_m as favorite_per_cnt,
    f.number as order_cnt,
    f.uv as order_uv,
    f.aver as order_per_cnt,
    (f.number/c.item_click_cnt_m) as cvr, 
    (f.uv/c.item_click_uv_m) as uv_cvr 
from (
    select 
        * 
    from 
        zaful_recommend.goods_exposure_day_country 
    where 
        glb_plf='m'
    ) b 
left join (
    select 
        country_code,
        goods_sn,
        count(*) as item_click_cnt_m,
        count(distinct glb_od) as item_click_uv_m,
        count(*)/count(distinct glb_od) as item_click_per_cnt_m 
    from 
        zaful_recommend.goods_click_day_country 
    where 
        glb_t = 'ic' 
        and glb_x in ('sku','addtobag') 
        and glb_od!='' 
        and get_json_object(glb_skuinfo,'$.sku')!='' 
        and glb_plf='m' 
    group by 
        country_code,
        goods_sn
    ) c 
on 
    b.goods_sn=c.goods_sn 
    and b.country_code=c.country_code 
left join (
    select 
        country_code,
        goods_sn,
        count(*) as item_cart_cnt_m,
        count(distinct glb_od) as item_cart_uv_m,
        count(*)/count(distinct glb_od) as item_cart_per_cnt_m 
    from 
        zaful_recommend.goods_click_day_country 
    where 
        glb_t = 'ic' 
        and glb_x = 'ADT' 
        and get_json_object(glb_skuinfo,'$.sku')!='' 
        and glb_plf='m' 
    group by 
        country_code,
        goods_sn
    ) d 
on 
    b.goods_sn=d.goods_sn 
    and b.country_code=d.country_code 
left join (
    select 
        country_code,
        goods_sn,
        count(*) as item_collected_cnt_m,
        count(distinct glb_od) as item_collected_uv_m,
        count(*)/count(distinct glb_od) as item_collected_per_cnt_m 
    from 
        zaful_recommend.goods_click_day_country 
    where 
        glb_t = 'ic' 
        and glb_x = 'ADF' 
        and glb_u!='' 
        and get_json_object(glb_skuinfo,'$.sku')!='' 
        and glb_plf='m' 
    group by 
        country_code,
        goods_sn
    ) e 
on 
    b.goods_sn=e.goods_sn 
    and b.country_code=e.country_code 
left join (
    select 
        * 
    from 
        zaful_recommend.goods_order_day_country 
    where 
        glb_plf='m'
    ) f 
on 
    b.goods_sn=f.goods_sn 
    and b.country_code=f.country_code
where 
    b.number is not null;


set mapred.job.queue.name = root.ai.offline;
--按天写分区表：pc平台按国家统计
insert overwrite table dw_zaful_recommend.feature_items_country_v1_1 partition (platform='pc', year=${YEAR},month=${MONTH},day=${DAY})  
select 
    item_id,
    country,
    (case when pv_cnt is not null then pv_cnt else 0 end) as pv_cnt,
    (case when pv_uv is not null then pv_uv else 0 end) as pv_uv,
    (case when pv_per_cnt is not null then pv_per_cnt else 0.0 end) as pv_per_cnt, 
    (case when ipv_cnt is not null then ipv_cnt else 0 end) as ipv_cnt,
    (case when ipv_uv is not null then ipv_uv else 0 end) as ipv_uv,
    (case when ipv_per_cnt is not null then ipv_per_cnt else 0.0 end) as ipv_per_cnt,
    (case when bag_cnt is not null then bag_cnt else 0 end) as bag_cnt,
    (case when bag_uv is not null then bag_uv else 0 end) as bag_uv,
    (case when bag_per_cnt is not null then bag_per_cnt else 0.0 end) as bag_per_cnt,
    (case when favorite_cnt is not null then favorite_cnt else 0 end) as favorite_cnt,
    (case when favorite_uv is not null then favorite_uv else 0 end) as favorite_uv,
    (case when favorite_per_cnt is not null then favorite_per_cnt else 0.0 end) as favorite_per_cnt,
    (case when order_cnt is not null then order_cnt else 0 end) as order_cnt,
    (case when order_uv is not null then order_uv else 0 end) as order_uv,
    (case when order_per_cnt is not null then order_per_cnt else 0.0 end) as order_per_cnt,
    (case when cvr is not null then cvr else 0.0 end) as cvr,
    (case when uv_cvr is not null then uv_cvr else 0.0 end) as uv_cvr
from zaful_recommend.item_info_pc_country11 
where 
    item_id is not null 
    and item_id<>'';


set mapred.job.queue.name = root.ai.offline;
--按天写分区表：m平台按国家统计
insert overwrite table dw_zaful_recommend.feature_items_country_v1_1 partition (platform='m', year=${YEAR},month=${MONTH},day=${DAY})  
select 
    item_id,
    country,
    (case when pv_cnt is not null then pv_cnt else 0 end) as pv_cnt,
    (case when pv_uv is not null then pv_uv else 0 end) as pv_uv,
    (case when pv_per_cnt is not null then pv_per_cnt else 0.0 end) as pv_per_cnt, 
    (case when ipv_cnt is not null then ipv_cnt else 0 end) as ipv_cnt,
    (case when ipv_uv is not null then ipv_uv else 0 end) as ipv_uv,
    (case when ipv_per_cnt is not null then ipv_per_cnt else 0.0 end) as ipv_per_cnt,
    (case when bag_cnt is not null then bag_cnt else 0 end) as bag_cnt,
    (case when bag_uv is not null then bag_uv else 0 end) as bag_uv,
    (case when bag_per_cnt is not null then bag_per_cnt else 0.0 end) as bag_per_cnt,
    (case when favorite_cnt is not null then favorite_cnt else 0 end) as favorite_cnt,
    (case when favorite_uv is not null then favorite_uv else 0 end) as favorite_uv,
    (case when favorite_per_cnt is not null then favorite_per_cnt else 0.0 end) as favorite_per_cnt,
    (case when order_cnt is not null then order_cnt else 0 end) as order_cnt,
    (case when order_uv is not null then order_uv else 0 end) as order_uv,
    (case when order_per_cnt is not null then order_per_cnt else 0.0 end) as order_per_cnt,
    (case when cvr is not null then cvr else 0.0 end) as cvr,
    (case when uv_cvr is not null then uv_cvr else 0.0 end) as uv_cvr
from zaful_recommend.item_info_m_country11 
where 
    item_id is not null 
    and item_id<>'';


set mapred.job.queue.name = root.ai.offline;
--app当天埋点数据
insert overwrite table zaful_recommend.app_click_day_country
select
  *
from
  ods.ods_app_burial_log
where
  site = 'zaful'
  and get_json_object(event_value, '$.af_inner_mediasource') != 'unknow mediasource'
  and year = ${YEAR}
  and month = ${MONTH}
  and day = ${DAY};


set mapred.job.queue.name = root.ai.offline;
--app当天曝光数据
insert overwrite table zaful_recommend.app_goods_exposure_country  
select 
   b.country_code,
   b.platform,
   b.goods,
   count(*) as number,
   count(distinct b.appsflyer_device_id) as uv,
   count(*)/count(distinct b.appsflyer_device_id) as aver 
from (
    select * 
    from (
        select 
            country_code,
            appsflyer_device_id,
            platform,
            get_json_object(event_value, '$.af_content_id') as skus 
        from 
            zaful_recommend.app_click_day_country 
        where 
            event_name='af_impression'
        ) a 
lateral view 
    explode(split(a.skus, ',')) myTable as goods) b 
group by 
    b.country_code,
    b.platform,
    b.goods;
 

set mapred.job.queue.name = root.ai.offline;
--app当天下单数据
insert overwrite table zaful_recommend.app_goods_order_country 
select 
    x.platform,
    x.country,
    x.goods_sn,
    x.number,
    x.uv,
    x.aver,
    y.region_code as country_code 
from (
    select 
        p.platform,
        p.country,
        p.goods_sn,
        sum(p.goods_number) as number,
        count(distinct p.user_id) as uv,
        sum(p.goods_number)/count(distinct p.user_id) as aver 
    from (
        select 
            'ios' as platform,
            f.country,
            f.user_id,
            g.goods_sn,
            g.goods_number 
        from (
            select 
                distinct * 
            from (
                select 
                    country,
                    order_id,
                    user_id 
                from 
                    stg.zaful_eload_order_info 
                where 
                    order_status!=0 and order_status!=11 
                    and from_unixtime(add_time,'yyyyMMdd')=${DATE} 
                    and (order_sn like 'UA%' or order_sn like 'UUA%')
                ) a
            ) f 
        join 
            stg.zaful_eload_order_goods g 
        on 
            f.order_id = g.order_id 
    union all 
        select 
            'android' as platform,
            m.country,
            m.user_id,
            n.goods_sn,
            n.goods_number 
        from (
            select 
                distinct * 
            from (
                select 
                    country,
                    order_id,
                    user_id 
                from 
                    stg.zaful_eload_order_info 
                where 
                    order_status!=0 and order_status!=11 
                    and from_unixtime(add_time,'yyyyMMdd')=${DATE} 
                    and (order_sn like 'UB%' or order_sn like 'UUB%')
                ) b
            ) m 
        join 
            stg.zaful_eload_order_goods n 
        on 
            m.order_id = n.order_id 
        ) p 
    group by 
        p.platform,
        p.country,
        p.goods_sn
    ) x 
join (
    select 
        distinct * 
    from (
        select 
            region_id,
            region_code 
        from 
            stg.zaful_eload_region
        ) q
    ) y 
on 
    x.country=y.region_id;


set mapred.job.queue.name = root.ai.offline;
--ios端数据整合
insert overwrite table zaful_recommend.item_info_ios_country 
select 
    b.goods as item_id,
    b.country_code,
    b.number as item_ios_pv_cnt,
    b.uv as item_ios_pv_uv,
    b.aver as item_ios_pv_per_cnt,
    c.item_click_cnt_ios as item_ios_ipv_cnt,
    c.item_click_uv_ios as item_ios_ipv_uv,
    c.item_click_per_cnt_ios as item_ios_ipv_per_cnt,
    d.item_cart_cnt_ios as item_ios_bag_cnt,
    d.item_cart_uv_ios as item_ios_bag_uv,
    d.item_cart_per_cnt_ios as item_ios_bag_per_cnt,
    e.item_collected_cnt_ios as item_ios_favorite_cnt,
    e.item_collected_uv_ios as item_ios_favorite_uv,
    e.item_collected_per_cnt_ios as item_ios_favorite_per_cnt,
    f.number as item_ios_order_cnt,
    f.uv as item_ios_order_uv, 
    f.aver as item_ios_order_per_cnt,
    (f.number/c.item_click_cnt_ios) as item_ios_cvr, 
    (f.uv/c.item_click_uv_ios) as item_ios_uv_cvr 
from (
    select 
        * 
    from 
        zaful_recommend.app_goods_exposure_country 
    where 
        platform='ios'
    ) b 
left join (
    select 
        country_code,
        get_json_object(event_value, '$.af_content_id') as goods_sn,
        count(*) as item_click_cnt_ios,
        count(distinct appsflyer_device_id) as item_click_uv_ios,
        count(*)/count(distinct appsflyer_device_id) as item_click_per_cnt_ios 
    from 
        zaful_recommend.app_click_day_country 
    where 
        event_name='af_view_product' 
        and get_json_object(event_value, '$.af_changed_size_or_color') = 0 
        and platform='ios' 
    group by 
        country_code,
        get_json_object(event_value, '$.af_content_id')
    ) c 
on 
    b.goods=c.goods_sn 
    and b.country_code=c.country_code 
left join (
    select 
        country_code,
        get_json_object(event_value, '$.af_content_id') as goods_sn,
        count(*) as item_cart_cnt_ios,
        count(distinct appsflyer_device_id) as item_cart_uv_ios,
        count(*)/count(distinct appsflyer_device_id) as item_cart_per_cnt_ios 
    from 
        zaful_recommend.app_click_day_country 
    where 
        event_name='af_add_to_bag' 
        and platform='ios' 
    group by 
        country_code,
        get_json_object(event_value, '$.af_content_id')
    ) d 
on 
    b.goods=d.goods_sn 
    and b.country_code=d.country_code
left join (
    select 
        country_code,
        get_json_object(event_value, '$.af_content_id') as goods_sn,
        count(*) as item_collected_cnt_ios,
        count(distinct appsflyer_device_id) as item_collected_uv_ios,
        count(*)/count(distinct appsflyer_device_id) as item_collected_per_cnt_ios 
    from 
        zaful_recommend.app_click_day_country 
    where 
        event_name='af_add_to_wishlist' 
        and platform='ios' 
    group by 
        country_code,
        get_json_object(event_value, '$.af_content_id')
    ) e 
on 
    b.goods=e.goods_sn 
    and b.country_code=e.country_code 
left join (
    select 
        * 
    from 
        zaful_recommend.app_goods_order_country 
    where 
        platform='ios'
    ) f 
on 
    b.goods=f.goods_sn 
    and b.country_code=f.country_code;


set mapred.job.queue.name = root.ai.offline;
--android端数据整合
insert overwrite table zaful_recommend.item_info_android_country 
select 
    b.goods as item_id,
    b.country_code,
    b.number as item_android_pv_cnt,
    b.uv as item_android_pv_uv,
    b.aver as item_android_pv_per_cnt,
    c.item_click_cnt_android as item_android_ipv_cnt,
    c.item_click_uv_android as item_android_ipv_uv,
    c.item_click_per_cnt_android as item_android_ipv_per_cnt,
    d.item_cart_cnt_android as item_android_bag_cnt,
    d.item_cart_uv_android as item_android_bag_uv,
    d.item_cart_per_cnt_android as item_android_bag_per_cnt,
    e.item_collected_cnt_android as item_android_favorite_cnt,
    e.item_collected_uv_android as item_android_favorite_uv,
    e.item_collected_per_cnt_android as item_android_favorite_per_cnt,
    f.number as item_android_order_cnt,
    f.uv as item_android_order_uv, 
    f.aver as item_android_order_per_cnt,
    (f.number/c.item_click_cnt_android) as item_android_cvr, 
    (f.uv/c.item_click_uv_android) as item_android_uv_cvr 
from (
    select 
        * 
    from 
        zaful_recommend.app_goods_exposure_country 
    where 
        platform='android'
    ) b 
left join (
    select 
        country_code,
        get_json_object(event_value, '$.af_content_id') as goods_sn,
        count(*) as item_click_cnt_android,
        count(distinct appsflyer_device_id) as item_click_uv_android,
        count(*)/count(distinct appsflyer_device_id) as item_click_per_cnt_android 
    from 
        zaful_recommend.app_click_day_country 
    where 
        event_name='af_view_product' 
        and get_json_object(event_value, '$.af_changed_size_or_color') = 0 
        and platform='android' 
    group by 
        country_code,
        get_json_object(event_value, '$.af_content_id')
    ) c 
on 
    b.goods=c.goods_sn 
    and b.country_code=c.country_code 
left join (
    select 
        country_code,
        get_json_object(event_value, '$.af_content_id') as goods_sn,
        count(*) as item_cart_cnt_android,
        count(distinct appsflyer_device_id) as item_cart_uv_android,
        count(*)/count(distinct appsflyer_device_id) as item_cart_per_cnt_android 
    from 
        zaful_recommend.app_click_day_country 
    where 
        event_name='af_add_to_bag' 
        and platform='android' 
    group by 
        country_code,
        get_json_object(event_value, '$.af_content_id')
    ) d 
on 
    b.goods=d.goods_sn 
    and b.country_code=d.country_code
left join (
    select 
        country_code,
        get_json_object(event_value, '$.af_content_id') as goods_sn,
        count(*) as item_collected_cnt_android,
        count(distinct appsflyer_device_id) as item_collected_uv_android,
        count(*)/count(distinct appsflyer_device_id) as item_collected_per_cnt_android 
    from 
        zaful_recommend.app_click_day_country 
    where 
        event_name='af_add_to_wishlist' 
        and platform='android' 
    group by 
        country_code,
        get_json_object(event_value, '$.af_content_id')
    ) e 
on 
    b.goods=e.goods_sn 
    and b.country_code=e.country_code 
left join (
    select 
        * 
    from 
        zaful_recommend.app_goods_order_country 
    where 
        platform='android'
    ) f 
on 
    b.goods=f.goods_sn 
    and b.country_code=f.country_code;


set mapred.job.queue.name = root.ai.offline;
--按天写分区表：ios平台按国家统计
insert overwrite table dw_zaful_recommend.feature_items_country_v1_1 partition (platform='ios', year=${YEAR},month=${MONTH},day=${DAY})  
select 
    item_id,
    country_code as country,
    (case when item_ios_pv_cnt is not null then item_ios_pv_cnt else 0 end) as pv_cnt,
    (case when item_ios_pv_uv is not null then item_ios_pv_uv else 0 end) as pv_uv,
    (case when item_ios_pv_per_cnt is not null then item_ios_pv_per_cnt else 0.0 end) as pv_per_cnt, 
    (case when item_ios_ipv_cnt is not null then item_ios_ipv_cnt else 0 end) as ipv_cnt,
    (case when item_ios_ipv_uv is not null then item_ios_ipv_uv else 0 end) as ipv_uv,
    (case when item_ios_ipv_per_cnt is not null then item_ios_ipv_per_cnt else 0.0 end) as ipv_per_cnt,
    (case when item_ios_bag_cnt is not null then item_ios_bag_cnt else 0 end) as bag_cnt,
    (case when item_ios_bag_uv is not null then item_ios_bag_uv else 0 end) as bag_uv,
    (case when item_ios_bag_per_cnt is not null then item_ios_bag_per_cnt else 0.0 end) as bag_per_cnt,
    (case when item_ios_favorite_cnt is not null then item_ios_favorite_cnt else 0 end) as favorite_cnt,
    (case when item_ios_favorite_uv is not null then item_ios_favorite_uv else 0 end) as favorite_uv,
    (case when item_ios_favorite_per_cnt is not null then item_ios_favorite_per_cnt else 0.0 end) as favorite_per_cnt,
    (case when item_ios_order_cnt is not null then item_ios_order_cnt else 0 end) as order_cnt,
    (case when item_ios_order_uv is not null then item_ios_order_uv else 0 end) as order_uv,
    (case when item_ios_order_per_cnt is not null then item_ios_order_per_cnt else 0.0 end) as order_per_cnt,
    (case when item_ios_cvr is not null then item_ios_cvr else 0.0 end) as cvr,
    (case when item_ios_uv_cvr is not null then item_ios_uv_cvr else 0.0 end) as uv_cvr
from zaful_recommend.item_info_ios_country 
where 
    item_id is not null 
    and item_id<>'';


set mapred.job.queue.name = root.ai.offline;
--按天写分区表：android平台按国家统计
insert overwrite table dw_zaful_recommend.feature_items_country_v1_1 partition (platform='android', year=${YEAR},month=${MONTH},day=${DAY})  
select 
    item_id,
    country_code as country,
    (case when item_android_pv_cnt is not null then item_android_pv_cnt else 0 end) as pv_cnt,
    (case when item_android_pv_uv is not null then item_android_pv_uv else 0 end) as pv_uv,
    (case when item_android_pv_per_cnt is not null then item_android_pv_per_cnt else 0.0 end) as pv_per_cnt, 
    (case when item_android_ipv_cnt is not null then item_android_ipv_cnt else 0 end) as ipv_cnt,
    (case when item_android_ipv_uv is not null then item_android_ipv_uv else 0 end) as ipv_uv,
    (case when item_android_ipv_per_cnt is not null then item_android_ipv_per_cnt else 0.0 end) as ipv_per_cnt,
    (case when item_android_bag_cnt is not null then item_android_bag_cnt else 0 end) as bag_cnt,
    (case when item_android_bag_uv is not null then item_android_bag_uv else 0 end) as bag_uv,
    (case when item_android_bag_per_cnt is not null then item_android_bag_per_cnt else 0.0 end) as bag_per_cnt,
    (case when item_android_favorite_cnt is not null then item_android_favorite_cnt else 0 end) as favorite_cnt,
    (case when item_android_favorite_uv is not null then item_android_favorite_uv else 0 end) as favorite_uv,
    (case when item_android_favorite_per_cnt is not null then item_android_favorite_per_cnt else 0.0 end) as favorite_per_cnt,
    (case when item_android_order_cnt is not null then item_android_order_cnt else 0 end) as order_cnt,
    (case when item_android_order_uv is not null then item_android_order_uv else 0 end) as order_uv,
    (case when item_android_order_per_cnt is not null then item_android_order_per_cnt else 0.0 end) as order_per_cnt,
    (case when item_android_cvr is not null then item_android_cvr else 0.0 end) as cvr,
    (case when item_android_uv_cvr is not null then item_android_uv_cvr else 0.0 end) as uv_cvr
from zaful_recommend.item_info_android_country 
where 
    item_id is not null 
    and item_id<>'';