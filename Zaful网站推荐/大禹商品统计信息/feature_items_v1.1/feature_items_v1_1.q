set mapred.job.queue.name = root.ai.offline;
--商品点击数据(天)
insert overwrite table zaful_recommend.goods_click_day 
    select 
        *,
        get_json_object(glb_skuinfo, '$.sku') as goods_sn 
    from stg.zf_pc_event_info 
    where 
        year=${YEAR} 
        and month=${MONTH}
        and day=${DAY};

set mapred.job.queue.name = root.ai.offline;
--商品曝光数据(天)
insert overwrite table zaful_recommend.goods_exposure_day 
    select 
        m.glb_plf,
        m.goods_sn,
        count(*) as number,
        count(distinct m.glb_od) as uv,
        count(*)/count(distinct m.glb_od) as aver 
    from (
        select 
            a.glb_od,
            a.glb_plf,
            get_json_object(b.glb_ubcta_col,'$.sku') as goods_sn 
        from (
            select 
                glb_od,
                log_id,
                glb_plf,
                year,
                month,
                day 
            from zaful_recommend.goods_click_day 
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
            from stg.zf_pc_event_ubcta_info 
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
    group by m.glb_plf,m.goods_sn;

set mapred.job.queue.name = root.ai.offline;
--商品下单数据(天)
insert overwrite table zaful_recommend.goods_order_day 
    select 
        p.glb_plf,
        p.goods_sn,
        sum(p.goods_number) as number,
        count(distinct p.user_id) as uv,
        sum(p.goods_number)/count(distinct p.user_id) as aver 
    from (
        select 
            'pc' as glb_plf,
            f.user_id,
            g.goods_sn,
            g.goods_number 
        from (
            select distinct * 
            from (
                select 
                    order_id,
                    user_id 
                from stg.zaful_eload_order_info 
                where 
                    order_status!=0 
                    and order_status!=11 
                    and from_unixtime(add_time,'yyyyMMdd')=${DATE} 
                    and (order_sn like 'UU1%' or order_sn like 'U1%')
                ) a
            ) f
        join stg.zaful_eload_order_goods g 
        on 
            f.order_id = g.order_id 
    union all 
        select 
            'm' as glb_plf,
            m.user_id,
            n.goods_sn,
            n.goods_number 
        from (
            select distinct * 
            from (
                select 
                    order_id,
                    user_id 
                from stg.zaful_eload_order_info 
                where 
                    order_status!=0 
                    and order_status!=11 
                    and from_unixtime(add_time,'yyyyMMdd')=${DATE}
                    and (order_sn like 'UL%' or order_sn like 'UM%')
                ) b
            ) m 
        join stg.zaful_eload_order_goods n 
        on 
            m.order_id = n.order_id) p 
    group by p.glb_plf,p.goods_sn;

set mapred.job.queue.name = root.ai.offline;
--pc端整合
insert overwrite table zaful_recommend.item_info_pc 
    select 
        b.goods_sn as item_id,
        b.number as item_pc_pv_cnt,
        b.uv as item_pc_pv_uv,
        b.aver as item_pc_pv_per_cnt,
        c.item_click_cnt_pc as item_pc_ipv_cnt,
        c.item_click_uv_pc as item_pc_ipv_uv,
        c.item_click_per_cnt_pc as item_pc_ipv_per_cnt,
        d.item_cart_cnt_pc as item_pc_bag_cnt,
        d.item_cart_uv_pc as item_pc_bag_uv,
        d.item_cart_per_cnt_pc as item_pc_bag_per_cnt,
        e.item_collected_cnt_pc as item_pc_favorite_cnt,
        e.item_collected_uv_pc as item_pc_favorite_uv,
        e.item_collected_per_cnt_pc as item_pc_favorite_per_cnt,
        f.number as item_pc_order_cnt,
        f.uv as item_pc_order_uv,
        f.aver as item_pc_order_per_cnt,
        (f.number/c.item_click_cnt_pc) as item_pc_cvr, 
        (f.uv/c.item_click_uv_pc) as item_pc_uv_cvr 
    from (
        select * 
        from zaful_recommend.goods_exposure_day 
        where glb_plf='pc'
        ) b 
    left join (
        select 
            goods_sn,
            count(*) as item_click_cnt_pc,
            count(distinct glb_od) as item_click_uv_pc,
            count(*)/count(distinct glb_od) as item_click_per_cnt_pc 
        from zaful_recommend.goods_click_day 
        where 
            glb_t = 'ic' 
            and glb_x in ('sku','addtobag') 
            and glb_od!='' 
            and get_json_object(glb_skuinfo,'$.sku')!='' 
            and glb_plf='pc' group by goods_sn
        ) c 
    on b.goods_sn=c.goods_sn 
    left join (
        select 
            goods_sn,
            count(*) as item_cart_cnt_pc,
            count(distinct glb_od) as item_cart_uv_pc,
            count(*)/count(distinct glb_od) as item_cart_per_cnt_pc 
        from zaful_recommend.goods_click_day 
        where 
            glb_t = 'ic' 
            and glb_x = 'ADT' 
            and get_json_object(glb_skuinfo,'$.sku')!='' 
            and glb_plf='pc' group by goods_sn
        ) d 
    on b.goods_sn=d.goods_sn 
    left join (
        select 
            goods_sn,
            count(*) as item_collected_cnt_pc,
            count(distinct glb_od) as item_collected_uv_pc,
            count(*)/count(distinct glb_od) as item_collected_per_cnt_pc 
        from 
            zaful_recommend.goods_click_day 
        where 
            glb_t = 'ic' 
            and glb_x = 'ADF' 
            and glb_u!='' 
            and get_json_object(glb_skuinfo,'$.sku')!='' 
            and glb_plf='pc' group by goods_sn
        ) e 
    on b.goods_sn=e.goods_sn 
    left join (
        select * 
        from zaful_recommend.goods_order_day 
        where glb_plf='pc'
        ) f 
    on b.goods_sn=f.goods_sn;

set mapred.job.queue.name = root.ai.offline;
--m端整合
insert overwrite table zaful_recommend.item_info_m 
    select 
        b.goods_sn as item_id,
        b.number as item_m_pv_cnt,
        b.uv as item_m_pv_uv,
        b.aver as item_m_pv_per_cnt,
        c.item_click_cnt_m as item_m_ipv_cnt,
        c.item_click_uv_m as item_m_ipv_uv,
        c.item_click_per_cnt_m as item_m_ipv_per_cnt,
        d.item_cart_cnt_m as item_m_bag_cnt,
        d.item_cart_uv_m as item_m_bag_uv,
        d.item_cart_per_cnt_m as item_m_bag_per_cnt,
        e.item_collected_cnt_m as item_m_favorite_cnt,
        e.item_collected_uv_m as item_m_favorite_uv,
        e.item_collected_per_cnt_m as item_m_favorite_per_cnt,
        f.number as item_m_order_cnt,
        f.uv as item_m_order_uv,
        f.aver as item_m_order_per_cnt,
        (f.number/c.item_click_cnt_m) as item_m_cvr, 
        (f.uv/c.item_click_uv_m) as item_m_uv_cvr 
    from (
        select * from zaful_recommend.goods_exposure_day where glb_plf='m') b 
    left join (
        select 
            goods_sn,
            count(*) as item_click_cnt_m,
            count(distinct glb_od) as item_click_uv_m,
            count(*)/count(distinct glb_od) as item_click_per_cnt_m 
        from zaful_recommend.goods_click_day 
        where 
            glb_t = 'ic' 
            and glb_x in ('sku','addtobag') 
            and glb_od!='' 
            and get_json_object(glb_skuinfo,'$.sku')!='' 
            and glb_plf='m' group by goods_sn
        ) c 
    on 
        b.goods_sn=c.goods_sn 
    left join (
        select 
            goods_sn,
            count(*) as item_cart_cnt_m,
            count(distinct glb_od) as item_cart_uv_m,
            count(*)/count(distinct glb_od) as item_cart_per_cnt_m 
        from zaful_recommend.goods_click_day 
        where 
            glb_t = 'ic' 
            and glb_x = 'ADT' 
            and get_json_object(glb_skuinfo,'$.sku')!='' 
            and glb_plf='m' 
        group by goods_sn
        ) d 
    on 
        b.goods_sn=d.goods_sn 
    left join (
        select 
            goods_sn,
            count(*) as item_collected_cnt_m,
            count(distinct glb_od) as item_collected_uv_m,
            count(*)/count(distinct glb_od) as item_collected_per_cnt_m 
        from zaful_recommend.goods_click_day 
        where 
            glb_t = 'ic' 
            and glb_x = 'ADF' 
            and glb_u!='' 
            and get_json_object(glb_skuinfo,'$.sku')!='' 
            and glb_plf='m' group by goods_sn
        ) e 
    on 
        b.goods_sn=e.goods_sn 
    left join (
        select * from zaful_recommend.goods_order_day where glb_plf='m') f 
    on 
        b.goods_sn=f.goods_sn;


set mapred.job.queue.name = root.ai.offline;
--按天写分区表：pc平台统计
insert overwrite table dw_zaful_recommend.feature_items_v1_1 partition (platform='pc', year=${YEAR},month=${MONTH},day=${DAY})  
select 
    item_id,
    (case when item_pc_pv_cnt is not null then item_pc_pv_cnt else 0 end) as pv_cnt,
    (case when item_pc_pv_uv is not null then item_pc_pv_uv else 0 end) as pv_uv,
    (case when item_pc_pv_per_cnt is not null then item_pc_pv_per_cnt else 0.0 end) as pv_per_cnt, 
    (case when item_pc_ipv_cnt is not null then item_pc_ipv_cnt else 0 end) as ipv_cnt,
    (case when item_pc_ipv_uv is not null then item_pc_ipv_uv else 0 end) as ipv_uv,
    (case when item_pc_ipv_per_cnt is not null then item_pc_ipv_per_cnt else 0.0 end) as ipv_per_cnt,
    (case when item_pc_bag_cnt is not null then item_pc_bag_cnt else 0 end) as bag_cnt,
    (case when item_pc_bag_uv is not null then item_pc_bag_uv else 0 end) as bag_uv,
    (case when item_pc_bag_per_cnt is not null then item_pc_bag_per_cnt else 0.0 end) as bag_per_cnt,
    (case when item_pc_favorite_cnt is not null then item_pc_favorite_cnt else 0 end) as favorite_cnt,
    (case when item_pc_favorite_uv is not null then item_pc_favorite_uv else 0 end) as favorite_uv,
    (case when item_pc_favorite_per_cnt is not null then item_pc_favorite_per_cnt else 0.0 end) as favorite_per_cnt,
    (case when item_pc_order_cnt is not null then item_pc_order_cnt else 0 end) as order_cnt,
    (case when item_pc_order_uv is not null then item_pc_order_uv else 0 end) as order_uv,
    (case when item_pc_order_per_cnt is not null then item_pc_order_per_cnt else 0.0 end) as order_per_cnt,
    (case when item_pc_cvr is not null then item_pc_cvr else 0.0 end) as cvr,
    (case when item_pc_uv_cvr is not null then item_pc_uv_cvr else 0.0 end) as uv_cvr
from zaful_recommend.item_info_pc 
where 
    item_id is not null 
    and item_id<>'';

set mapred.job.queue.name = root.ai.offline;
--按天写分区表：m平台统计
insert overwrite table dw_zaful_recommend.feature_items_v1_1 partition (platform='m', year=${YEAR},month=${MONTH},day=${DAY})  
select 
    item_id,
    (case when item_m_pv_cnt is not null then item_m_pv_cnt else 0 end) as pv_cnt,
    (case when item_m_pv_uv is not null then item_m_pv_uv else 0 end) as pv_uv,
    (case when item_m_pv_per_cnt is not null then item_m_pv_per_cnt else 0.0 end) as pv_per_cnt, 
    (case when item_m_ipv_cnt is not null then item_m_ipv_cnt else 0 end) as ipv_cnt,
    (case when item_m_ipv_uv is not null then item_m_ipv_uv else 0 end) as ipv_uv,
    (case when item_m_ipv_per_cnt is not null then item_m_ipv_per_cnt else 0.0 end) as ipv_per_cnt,
    (case when item_m_bag_cnt is not null then item_m_bag_cnt else 0 end) as bag_cnt,
    (case when item_m_bag_uv is not null then item_m_bag_uv else 0 end) as bag_uv,
    (case when item_m_bag_per_cnt is not null then item_m_bag_per_cnt else 0.0 end) as bag_per_cnt,
    (case when item_m_favorite_cnt is not null then item_m_favorite_cnt else 0 end) as favorite_cnt,
    (case when item_m_favorite_uv is not null then item_m_favorite_uv else 0 end) as favorite_uv,
    (case when item_m_favorite_per_cnt is not null then item_m_favorite_per_cnt else 0.0 end) as favorite_per_cnt,
    (case when item_m_order_cnt is not null then item_m_order_cnt else 0 end) as order_cnt,
    (case when item_m_order_uv is not null then item_m_order_uv else 0 end) as order_uv,
    (case when item_m_order_per_cnt is not null then item_m_order_per_cnt else 0.0 end) as order_per_cnt,
    (case when item_m_cvr is not null then item_m_cvr else 0.0 end) as cvr,
    (case when item_m_uv_cvr is not null then item_m_uv_cvr else 0.0 end) as uv_cvr
from zaful_recommend.item_info_m 
where 
    item_id is not null 
    and item_id<>'';


set mapred.job.queue.name = root.ai.offline;
--商品点击数据(天)
insert overwrite table zaful_recommend.app_click_day 
select 
    * 
from 
    ods.ods_app_burial_log 
where 
    site='zaful' 
	and get_json_object(event_value, '$.af_inner_mediasource') !='unknow mediasource'
    and year=${YEAR} 
    and month=${MONTH} 
    and day =${DAY};


set mapred.job.queue.name = root.ai.offline;
--商品曝光数据
insert overwrite table zaful_recommend.app_goods_exposure 
select 
    b.platform,
    b.goods,
    count(*) as number,
    count(distinct b.appsflyer_device_id) as uv,
    count(*)/count(distinct b.appsflyer_device_id) as aver 
from (
    select 
        * 
    from (
        select 
            appsflyer_device_id,
            platform,
            get_json_object(event_value, '$.af_content_id') as skus 
        from 
            zaful_recommend.app_click_day 
        where 
            event_name='af_impression'
        ) a 
    lateral view explode(split(a.skus, ',')) myTable as goods
    ) b 
group by 
    b.platform,
    b.goods;

set mapred.job.queue.name = root.ai.offline;
--商品下单数据
insert overwrite table zaful_recommend.app_goods_order 
select 
    p.platform,
    p.goods_sn,
    sum(p.goods_number) as number,
    count(distinct p.user_id) as uv,
    sum(p.goods_number)/count(distinct p.user_id) as aver 
from (
    select 
        'ios' as platform,
        f.user_id,
        g.goods_sn,
        g.goods_number 
    from (
        select 
            distinct * 	
        from (
            select 
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
        m.user_id,
        n.goods_sn,
        n.goods_number 
    from (
        select 
            distinct * 
        from (
            select 
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
    p.goods_sn;


set mapred.job.queue.name = root.ai.offline;
--ios数据
insert overwrite table zaful_recommend.item_info_ios 
select 
    b.goods as item_id,
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
    from zaful_recommend.app_goods_exposure 
    where platform='ios'
    ) b 
left join (
    select 
        get_json_object(event_value, '$.af_content_id') as goods_sn,
        count(*) as item_click_cnt_ios,
        count(distinct appsflyer_device_id) as item_click_uv_ios,
        count(*)/count(distinct appsflyer_device_id) as item_click_per_cnt_ios 
    from zaful_recommend.app_click_day 
    where 
        event_name='af_view_product' 
        and get_json_object(event_value, '$.af_changed_size_or_color') = 0 
        and platform='ios' 
    group by get_json_object(event_value, '$.af_content_id')
    ) c 
on b.goods=c.goods_sn 
left join (
    select 
        get_json_object(event_value, '$.af_content_id') as goods_sn,
        count(*) as item_cart_cnt_ios,
        count(distinct appsflyer_device_id) as item_cart_uv_ios,
        count(*)/count(distinct appsflyer_device_id) as item_cart_per_cnt_ios 
    from zaful_recommend.app_click_day 
    where 
        event_name='af_add_to_bag' 
        and platform='ios' 
    group by get_json_object(event_value, '$.af_content_id')
    ) d 
on b.goods=d.goods_sn 
left join (
    select 
        get_json_object(event_value, '$.af_content_id') as goods_sn,
        count(*) as item_collected_cnt_ios,
        count(distinct appsflyer_device_id) as item_collected_uv_ios,
        count(*)/count(distinct appsflyer_device_id) as item_collected_per_cnt_ios 
    from zaful_recommend.app_click_day 
    where 
        event_name='af_add_to_wishlist' 
        and platform='ios' 
    group by get_json_object(event_value, '$.af_content_id')
    ) e 
on b.goods=e.goods_sn 
left join (
    select 
        * 
    from zaful_recommend.app_goods_order 
    where platform='ios'
    ) f 
on b.goods=f.goods_sn;


set mapred.job.queue.name = root.ai.offline;
--android数据
insert overwrite table zaful_recommend.item_info_android 
select 
    b.goods as item_id,
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
    from zaful_recommend.app_goods_exposure 
    where platform='android'
    ) b 
left join (
    select 
        get_json_object(event_value, '$.af_content_id') as goods_sn,
        count(*) as item_click_cnt_android,
        count(distinct appsflyer_device_id) as item_click_uv_android,
        count(*)/count(distinct appsflyer_device_id) as item_click_per_cnt_android 
    from zaful_recommend.app_click_day 
    where 
        event_name='af_view_product' 
        and get_json_object(event_value, '$.af_changed_size_or_color') = 0 
        and platform='android' 
    group by get_json_object(event_value, '$.af_content_id')
    ) c 
on b.goods=c.goods_sn 
left join (
    select 
        get_json_object(event_value, '$.af_content_id') as goods_sn,
        count(*) as item_cart_cnt_android,
        count(distinct appsflyer_device_id) as item_cart_uv_android,
        count(*)/count(distinct appsflyer_device_id) as item_cart_per_cnt_android 
    from zaful_recommend.app_click_day 
    where 
        event_name='af_add_to_bag' 
        and platform='android' 
    group by get_json_object(event_value, '$.af_content_id')
    ) d 
on b.goods=d.goods_sn 
left join (
    select 
        get_json_object(event_value, '$.af_content_id') as goods_sn,
        count(*) as item_collected_cnt_android,
        count(distinct appsflyer_device_id) as item_collected_uv_android,
        count(*)/count(distinct appsflyer_device_id) as item_collected_per_cnt_android 
    from zaful_recommend.app_click_day 
    where 
        event_name='af_add_to_wishlist' 
        and platform='android' 
    group by get_json_object(event_value, '$.af_content_id')
    ) e 
on b.goods=e.goods_sn 
left join (
    select 
        * 
    from zaful_recommend.app_goods_order 
    where platform='android'
    ) f 
on b.goods=f.goods_sn;


set mapred.job.queue.name = root.ai.offline;
--按天写分区表：ios平台统计
insert overwrite table dw_zaful_recommend.feature_items_v1_1 partition (platform='ios',year=${YEAR},month=${MONTH},day=${DAY})  
select 
    item_id,
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
from zaful_recommend.item_info_ios 
where 
    item_id is not null 
    and item_id<>'';


set mapred.job.queue.name = root.ai.offline;
--按天写分区表：android平台统计
insert overwrite table dw_zaful_recommend.feature_items_v1_1 partition (platform='android',year=${YEAR},month=${MONTH},day=${DAY})  
select 
    item_id,
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
from zaful_recommend.item_info_android 
where 
    item_id is not null 
    and item_id<>'';