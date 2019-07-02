SET mapred.job.name=feature_items_country_v2_2_ods;
set mapred.job.queue.name = root.ai.offline;
--商品点击数据(天)
insert overwrite table temp_zaful_recommend.goods_click_day_country
select 
    behaviour_type,	
    user_name,	
    search_input_word,	
    search_type,	
    click_times,	
    search_result_word,	
    referer,	
    cookie_id,	
    page_module,	
    page_sku,	
    osr_landing_url,	
    page_sub_type,	
    time_local,	
    user_ip,	
    user_agent,	
    real_client_ip,	
    accept_language,	
    activity_template,	
    sub_event_info,	
    session_id,	
    platform,	
    country_code,	
    search_suk_type,	
    page_info,	
    url_suffix,	
    page_stay_time,	
    skuinfo,	
    country_name,	
    login_status,	
    sku_warehouse_info,	
    country_number,	
    page_main_type,	
    last_page_url,	
    link_id,	
    sent_bytes_size,	
    user_id,	
    time_stamp,	
    sub_event_field,	
    search_click_position,	
    osr_referer_url,	
    current_page_url,	
    site_code,	
    page_code,	
    log_id,	
    other,	
    bts,	
    fingerprint,	
    unix_time,	
    year,	
    month,	
    day,	
    site,
    get_json_object(skuinfo, '$.sku') as goods_sn 
from 
    ods.ods_pc_burial_log 
where 
	site='zaful'
    and year=${YEAR} 
    and month=${MONTH} 
    and day=${DAY};

set mapred.job.queue.name = root.ai.offline;
--商品曝光数据(天)
insert overwrite table temp_zaful_recommend.goods_exposure_day_country 
select 
    m.platform,
    m.country_code,
    m.goods_sn,
    count(*) as number,
    count(distinct m.cookie_id) as uv,
    count(*)/count(distinct m.cookie_id) as aver 
from (
    select 
        a.cookie_id,
        a.country_code,
        a.platform,
        get_json_object(b.sub_event_field,'$.sku') as goods_sn 
    from (
        select 
            cookie_id,
            country_code,
            log_id,
            platform,
            year,
            month,
            day 
        from 
            temp_zaful_recommend.goods_click_day_country 
        where 
            behaviour_type = 'ie' 
            and sub_event_field!=''
        ) a 
    join (
        select 
            log_id,
            sub_event_field,
            year,
            month,
            day 
        from 
            ods.ods_pc_burial_log_ubcta 
        where 
		    site='zaful'
            and year=${YEAR}
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
    m.platform,
    m.country_code,
    m.goods_sn;

set mapred.job.queue.name = root.ai.offline;
--商品下单数据(天)
insert overwrite table temp_zaful_recommend.goods_order_day_country 
select 
    x.platform,
    x.country_code,
    x.goods_sn,
    sum(x.goods_number) as number,
    count(distinct x.user_id) as uv,
    sum(x.goods_number)/count(distinct x.user_id) as aver 
from (
    select 
        p.platform,
        p.user_id,
        p.goods_sn,
        p.goods_number,
        q.country_code 
    from (
        select 
            'pc' as platform,
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
                    ods.ods_m_zaful_eload_order_info 
                where 
                    --dt = ${DATE} 
                    dt='${DATE}'
                    and order_status!=0 and order_status!=11 
                    and from_unixtime(add_time+ 8 * 3600,'yyyyMMdd')=${DATE} 
                    and (order_sn like 'UU1%' or order_sn like 'U1%')
                ) a
            ) f 
        join 
            (select * from ods.ods_m_zaful_eload_order_goods where dt='${DATE}') g --dt = ${DATE}
        on 
            f.order_id = g.order_id 
    union all 
        select 
            'm' as platform,
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
                    ods.ods_m_zaful_eload_order_info 
                where 
                    --dt = ${DATE} 
                    dt='${DATE}'
                    and order_status!=0 and order_status!=11 
                    and from_unixtime(add_time+ 8 * 3600,'yyyyMMdd')=${DATE} 
                    and (order_sn like 'UL%' or order_sn like 'UM%')
                ) b
            ) m 
        join 
            (select * from ods.ods_m_zaful_eload_order_goods where dt='${DATE}') n --dt = ${DATE}
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
    x.platform,
    x.country_code,
    x.goods_sn;


set mapred.job.queue.name = root.ai.offline;
--pc端整合
insert overwrite table temp_zaful_recommend.item_info_pc_country11 
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
        temp_zaful_recommend.goods_exposure_day_country 
    where 
        platform='pc'
    ) b 
left join (
    select 
        country_code,
        goods_sn,
        count(*) as item_click_cnt_pc,
        count(distinct cookie_id) as item_click_uv_pc,
        count(*)/count(distinct cookie_id) as item_click_per_cnt_pc 
    from 
        temp_zaful_recommend.goods_click_day_country
    where 
        behaviour_type = 'ic' 
        and sub_event_info in ('sku','addtobag') 
        and cookie_id!='' 
        and get_json_object(skuinfo,'$.sku')!='' 
        and platform='pc' 
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
        count(distinct cookie_id) as item_cart_uv_pc,
        count(*)/count(distinct cookie_id) as item_cart_per_cnt_pc 
    from 
        temp_zaful_recommend.goods_click_day_country
    where 
        behaviour_type = 'ic' 
        and sub_event_info = 'ADT' 
        and get_json_object(skuinfo,'$.sku')!='' 
        and platform='pc' 
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
        count(distinct cookie_id) as item_collected_uv_pc,
        count(*)/count(distinct cookie_id) as item_collected_per_cnt_pc 
    from 
        temp_zaful_recommend.goods_click_day_country 
    where 
        behaviour_type = 'ic' 
        and sub_event_info = 'ADF' 
        and user_id!='' 
        and get_json_object(skuinfo,'$.sku')!='' 
        and platform='pc' 
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
        temp_zaful_recommend.goods_order_day_country 
    where 
        platform='pc'
    ) f 
on 
    b.goods_sn=f.goods_sn 
    and b.country_code=f.country_code  
where b.number is not null;


set mapred.job.queue.name = root.ai.offline;
--m端整合
insert overwrite table temp_zaful_recommend.item_info_m_country11 
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
        temp_zaful_recommend.goods_exposure_day_country 
    where 
        platform='m'
    ) b 
left join (
    select 
        country_code,
        goods_sn,
        count(*) as item_click_cnt_m,
        count(distinct cookie_id) as item_click_uv_m,
        count(*)/count(distinct cookie_id) as item_click_per_cnt_m 
    from 
        temp_zaful_recommend.goods_click_day_country 
    where 
        behaviour_type = 'ic' 
        and sub_event_info in ('sku','addtobag') 
        and cookie_id!='' 
        and get_json_object(skuinfo,'$.sku')!='' 
        and platform='m' 
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
        count(distinct cookie_id) as item_cart_uv_m,
        count(*)/count(distinct cookie_id) as item_cart_per_cnt_m 
    from 
        temp_zaful_recommend.goods_click_day_country 
    where 
        behaviour_type = 'ic' 
        and sub_event_info = 'ADT' 
        and get_json_object(skuinfo,'$.sku')!='' 
        and platform='m' 
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
        count(distinct cookie_id) as item_collected_uv_m,
        count(*)/count(distinct cookie_id) as item_collected_per_cnt_m 
    from 
        temp_zaful_recommend.goods_click_day_country 
    where 
        behaviour_type = 'ic' 
        and sub_event_info = 'ADF' 
        and user_id!='' 
        and get_json_object(skuinfo,'$.sku')!='' 
        and platform='m' 
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
        temp_zaful_recommend.goods_order_day_country 
    where 
        platform='m'
    ) f 
on 
    b.goods_sn=f.goods_sn 
    and b.country_code=f.country_code 
where 
    b.number is not null;


set mapred.job.queue.name = root.ai.offline;
--按天写分区表：pc平台按国家统计
insert overwrite table dw_zaful_recommend.feature_items_country_v2_2_ods partition (platform='pc', year=${YEAR},month=${MONTH},day=${DAY})  
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
from temp_zaful_recommend.item_info_pc_country11 
where 
    item_id is not null 
    and item_id<>'';


set mapred.job.queue.name = root.ai.offline;
--按天写分区表：m平台按国家统计
insert overwrite table dw_zaful_recommend.feature_items_country_v2_2_ods partition (platform='m', year=${YEAR},month=${MONTH},day=${DAY})  
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
from temp_zaful_recommend.item_info_m_country11 
where 
    item_id is not null 
    and item_id<>'';

set mapred.job.queue.name = root.ai.offline;
--app当天埋点数据
insert overwrite table temp_zaful_recommend.app_click_day_country 
select
attributed_touch_type
,attributed_touch_time
,event_type
,attribution_type
,click_time
,download_time
,install_time
,media_source
,agency
,af_channel
,af_keywords
,campaign
,af_c_id
,af_adset
,af_adset_id
,af_ad
,af_ad_id
,fb_campaign_name
,fb_campaign_id
,fb_adset_name
,fb_adset_id
,fb_adgroup_name
,fb_adgroup_id
,af_ad_type
,af_siteid
,af_sub1
,af_sub2
,af_sub3
,af_sub4
,af_sub5
,http_referrer
,click_url
,af_cost_model
,af_cost_value
,af_cost_currency
,cost_per_install
,is_retargeting
,re_targeting_conversion_type
,country_code
,city
,ip
,wifi
,mac
,operator
,carrier
,language
,appsflyer_device_id
,advertising_id
,android_id
,customer_user_id
,imei
,idfa
,platform
,device_brand
,device_model
,os_version
,app_version
,sdk_version
,app_id
,app_name
,bundle_id
,event_time
,event_name
,event_value
,currency
,download_time_selected_timezone
,click_time_selected_timezone
,install_time_selected_timezone
,event_time_selected_timezone
,selected_currency
,revenue_in_selected_currency
,cost_in_selected_currency
,other
,unix_time
,year
,month
,day
,site
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
insert overwrite table temp_zaful_recommend.app_goods_exposure_country  
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
            temp_zaful_recommend.app_click_day_country 
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
insert overwrite table temp_zaful_recommend.app_goods_order_country 
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
                    ods.ods_m_zaful_eload_order_info 
                where 
                    --dt = ${DATE} 
                    dt='${DATE}'
                    and order_status!=0 and order_status!=11 
                    and from_unixtime(add_time+ 8 * 3600,'yyyyMMdd')=${DATE} 
                    and (order_sn like 'UA%' or order_sn like 'UUA%')
                ) a
            ) f 
        join 
            (select * from ods.ods_m_zaful_eload_order_goods where dt='${DATE}') g  --dt = ${DATE}
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
                    ods.ods_m_zaful_eload_order_info 
                where 
                    --dt = ${DATE} 
                    dt='${DATE}'
                    and order_status!=0 and order_status!=11 
                    and from_unixtime(add_time+ 8 * 3600,'yyyyMMdd')=${DATE} 
                    and (order_sn like 'UB%' or order_sn like 'UUB%')
                ) b
            ) m 
        join 
            (select * from ods.ods_m_zaful_eload_order_goods where dt='${DATE}') n  --dt = ${DATE}
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
insert overwrite table temp_zaful_recommend.item_info_ios_country 
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
        temp_zaful_recommend.app_goods_exposure_country 
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
        temp_zaful_recommend.app_click_day_country 
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
        temp_zaful_recommend.app_click_day_country 
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
        temp_zaful_recommend.app_click_day_country 
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
        temp_zaful_recommend.app_goods_order_country 
    where 
        platform='ios'
    ) f 
on 
    b.goods=f.goods_sn 
    and b.country_code=f.country_code;


set mapred.job.queue.name = root.ai.offline;
--android端数据整合
insert overwrite table temp_zaful_recommend.item_info_android_country 
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
        temp_zaful_recommend.app_goods_exposure_country 
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
        temp_zaful_recommend.app_click_day_country 
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
        temp_zaful_recommend.app_click_day_country 
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
        temp_zaful_recommend.app_click_day_country 
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
        temp_zaful_recommend.app_goods_order_country 
    where 
        platform='android'
    ) f 
on 
    b.goods=f.goods_sn 
    and b.country_code=f.country_code;


set mapred.job.queue.name = root.ai.offline;
--按天写分区表：ios平台按国家统计
insert overwrite table dw_zaful_recommend.feature_items_country_v2_2_ods partition (platform='ios', year=${YEAR},month=${MONTH},day=${DAY})  
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
from temp_zaful_recommend.item_info_ios_country 
where 
    item_id is not null 
    and item_id<>'';


set mapred.job.queue.name = root.ai.offline;
--按天写分区表：android平台按国家统计
insert overwrite table dw_zaful_recommend.feature_items_country_v2_2_ods partition (platform='android', year=${YEAR},month=${MONTH},day=${DAY})  
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
from temp_zaful_recommend.item_info_android_country 
where 
    item_id is not null 
    and item_id<>'';
	

CREATE TABLE IF NOT EXISTS dw_zaful_recommend.feature_items_country_v2_2_ods_info (
    item_id String COMMENT '商品sku编号',
    country String COMMENT '国家',
    pv_cnt bigint COMMENT '商品的pc端曝光数',
    pv_uv bigint COMMENT '商品的pc端曝光uv',
    pv_per_cnt double COMMENT '商品的pc端人均曝光数',    
    ipv_cnt bigint COMMENT '商品的pc端点击数',
    ipv_uv bigint COMMENT '商品的pc端点击uv',
    ipv_per_cnt double COMMENT '商品的pc端人均点击数',
    bag_cnt bigint COMMENT '商品的pc端加购数',
    bag_uv bigint COMMENT '商品的pc端加购uv',
    bag_per_cnt double COMMENT '商品的pc端人均加购数',
    favorite_cnt bigint COMMENT '商品的pc端收藏数',
    favorite_uv bigint COMMENT '商品的pc端收藏uv',
    favorite_per_cnt double COMMENT '商品的pc端人均收藏数',
    order_item_cnt bigint COMMENT '商品的pc端销量(下单商品数)',
    order_uv  bigint COMMENT '商品的pc端下单uv',
    order_per_cnt double COMMENT '商品的pc端人均下单商品数',
    cvr double COMMENT '商品的pc端购买转化率',
    uv_cvr double COMMENT '商品的pc端购买uv转化率',
    order_cnt bigint COMMENT '商品的下单数',
    order_income bigint COMMENT '商品的收入',
    ctr double COMMENT '商品的点击率',
    uv_ctr double COMMENT '商品的UV点击率',
    platform  String COMMENT '平台'
    )
PARTITIONED BY (year String, month String, day String)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' LINES TERMINATED BY '\n' 
STORED AS TEXTFILE;


insert overwrite table dw_zaful_recommend.feature_items_country_v2_2_ods_info partition (year='${YEAR}',month='${MONTH}',day='${DAY}')  
select coalesce(a.item_id,b.sku) as item_id,coalesce(a.country,b.country_code) as country,
a.pv_cnt,a.pv_uv,a.pv_per_cnt,a.ipv_cnt,a.ipv_uv,a.ipv_per_cnt,a.bag_cnt,a.bag_uv,a.bag_per_cnt,a.favorite_cnt,a.favorite_uv,a.favorite_per_cnt,
a.order_item_cnt,a.order_uv,a.order_per_cnt,a.cvr,a.uv_cvr,
b.create_order_sum as order_cnt,b.gmv_num as order_income,
a.ipv_cnt/a.pv_cnt as ctr,a.ipv_uv/a.pv_uv as uv_ctr,
coalesce(a.platform,b.platform) as platform
from 
    (
    select item_id,country,
    pv_cnt,pv_uv,pv_per_cnt,ipv_cnt,ipv_uv,ipv_per_cnt,bag_cnt,bag_uv,bag_per_cnt,favorite_cnt,favorite_uv,favorite_per_cnt,
    order_item_cnt,order_uv,order_per_cnt,cvr,uv_cvr,platform
    from 
    dw_zaful_recommend.feature_items_country_v2_2_ods
    where concat(year,month,day) = '${DATE}'
    ) a
    full join
    (
    select sku,create_order_sum,gmv_num,platform,country_code
    from 
    dw_proj.feature_items_country_v2_2_ods_order
    ) b
    on a.item_id=b.sku and a.platform=b.platform and a.country=b.country_code
;
------------------------------------------------------------------------------------------------------------
--3天聚合
-------------------------------------------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS dw_zaful_recommend.feature_items_country_v2_2_ods_info_three_days (
    item_id String COMMENT '商品sku编号',
    country String COMMENT '国家',
    pv_cnt bigint COMMENT '商品的pc端曝光数',
    pv_uv bigint COMMENT '商品的pc端曝光uv',
    pv_per_cnt double COMMENT '商品的pc端人均曝光数',    
    ipv_cnt bigint COMMENT '商品的pc端点击数',
    ipv_uv bigint COMMENT '商品的pc端点击uv',
    ipv_per_cnt double COMMENT '商品的pc端人均点击数',
    bag_cnt bigint COMMENT '商品的pc端加购数',
    bag_uv bigint COMMENT '商品的pc端加购uv',
    bag_per_cnt double COMMENT '商品的pc端人均加购数',
    favorite_cnt bigint COMMENT '商品的pc端收藏数',
    favorite_uv bigint COMMENT '商品的pc端收藏uv',
    favorite_per_cnt double COMMENT '商品的pc端人均收藏数',
    order_item_cnt bigint COMMENT '商品的pc端销量(下单商品数)',
    order_uv  bigint COMMENT '商品的pc端下单uv',
    order_per_cnt double COMMENT '商品的pc端人均下单商品数',
    cvr double COMMENT '商品的pc端购买转化率',
    uv_cvr double COMMENT '商品的pc端购买uv转化率',
    order_cnt bigint COMMENT '商品的下单数',
    order_income bigint COMMENT '商品的收入',
    ctr double COMMENT '商品的点击率',
    uv_ctr double COMMENT '商品的UV点击率',
    platform  String COMMENT '平台'
    )
PARTITIONED BY (year String, month String, day String)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' LINES TERMINATED BY '\n' 
STORED AS TEXTFILE;


insert overwrite table dw_zaful_recommend.feature_items_country_v2_2_ods_info_three_days partition (year='${YEAR}',month='${MONTH}',day='${DAY}')  
select 
item_id,
country,
sum(pv_cnt) as pv_cnt,
null,--count(distinct pv_uv),
null,--sum(pv_cnt)/count(distinct pv_uv) as pv_per_cnt,
sum(ipv_cnt) as ipv_cnt,
null,--count(distinct ipv_uv),
null,--sum(ipv_cnt)/count(distinct ipv_uv) as ipv_per_cnt,
sum(bag_cnt) as bag_cnt,
null,--count(distinct bag_uv) as bag_uv,
null,--sum(bag_cnt)/count(distinct bag_uv) as bag_per_cnt,
sum(favorite_cnt) as favorite_cnt,
null,--count(distinct favorite_uv) as favorite_uv,
null,--sum(favorite_cnt)/count(distinct favorite_uv) as favorite_per_cnt,
sum(order_item_cnt) as order_item_cnt,
null,--count(distinct order_uv) as order_uv,
null,--sum(order_item_cnt)/count(distinct order_uv) as order_per_cnt,
sum(order_item_cnt)/sum(ipv_cnt) as cvr,
null,--count(distinct order_uv)/count(distinct ipv_uv) as uv_cvr,
sum(order_cnt) as order_cnt,
sum(order_income) as order_income,
sum(ipv_cnt)/sum(pv_cnt) as ctr,
null,--count(distinct ipv_uv)/count(distinct pv_uv) as uv_ctr,
platform
from 
dw_zaful_recommend.feature_items_country_v2_2_ods_info
where 
concat(year,'-',month,'-',day) between date_sub('${DATE_W}',2) and '${DATE_W}'
group by item_id,platform,country
;
------------------------------------------------------------------------------------------------------------
--7天聚合
-------------------------------------------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS dw_zaful_recommend.feature_items_country_v2_2_ods_info_seven_days (
    item_id String COMMENT '商品sku编号',
    country String COMMENT '国家',
    pv_cnt bigint COMMENT '商品的pc端曝光数',
    pv_uv bigint COMMENT '商品的pc端曝光uv',
    pv_per_cnt double COMMENT '商品的pc端人均曝光数',    
    ipv_cnt bigint COMMENT '商品的pc端点击数',
    ipv_uv bigint COMMENT '商品的pc端点击uv',
    ipv_per_cnt double COMMENT '商品的pc端人均点击数',
    bag_cnt bigint COMMENT '商品的pc端加购数',
    bag_uv bigint COMMENT '商品的pc端加购uv',
    bag_per_cnt double COMMENT '商品的pc端人均加购数',
    favorite_cnt bigint COMMENT '商品的pc端收藏数',
    favorite_uv bigint COMMENT '商品的pc端收藏uv',
    favorite_per_cnt double COMMENT '商品的pc端人均收藏数',
    order_item_cnt bigint COMMENT '商品的pc端销量(下单商品数)',
    order_uv  bigint COMMENT '商品的pc端下单uv',
    order_per_cnt double COMMENT '商品的pc端人均下单商品数',
    cvr double COMMENT '商品的pc端购买转化率',
    uv_cvr double COMMENT '商品的pc端购买uv转化率',
    order_cnt bigint COMMENT '商品的下单数',
    order_income bigint COMMENT '商品的收入',
    ctr double COMMENT '商品的点击率',
    uv_ctr double COMMENT '商品的UV点击率',
    platform  String COMMENT '平台'
    )
PARTITIONED BY (year String, month String, day String)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' LINES TERMINATED BY '\n' 
STORED AS TEXTFILE;


insert overwrite table dw_zaful_recommend.feature_items_country_v2_2_ods_info_seven_days partition (year='${YEAR}',month='${MONTH}',day='${DAY}')  
select 
item_id,
country,
sum(pv_cnt) as pv_cnt,
null,--count(distinct pv_uv),
null,--sum(pv_cnt)/count(distinct pv_uv) as pv_per_cnt,
sum(ipv_cnt) as ipv_cnt,
null,--count(distinct ipv_uv),
null,--sum(ipv_cnt)/count(distinct ipv_uv) as ipv_per_cnt,
sum(bag_cnt) as bag_cnt,
null,--count(distinct bag_uv) as bag_uv,
null,--sum(bag_cnt)/count(distinct bag_uv) as bag_per_cnt,
sum(favorite_cnt) as favorite_cnt,
null,--count(distinct favorite_uv) as favorite_uv,
null,--sum(favorite_cnt)/count(distinct favorite_uv) as favorite_per_cnt,
sum(order_item_cnt) as order_item_cnt,
null,--count(distinct order_uv) as order_uv,
null,--sum(order_item_cnt)/count(distinct order_uv) as order_per_cnt,
sum(order_item_cnt)/sum(ipv_cnt) as cvr,
null,--count(distinct order_uv)/count(distinct ipv_uv) as uv_cvr,
sum(order_cnt) as order_cnt,
sum(order_income) as order_income,
sum(ipv_cnt)/sum(pv_cnt) as ctr,
null,--count(distinct ipv_uv)/count(distinct pv_uv) as uv_ctr,
platform
from 
dw_zaful_recommend.feature_items_country_v2_2_ods_info
where 
concat(year,'-',month,'-',day) between date_sub('${DATE_W}',6) and '${DATE_W}'
group by item_id,platform,country
;
------------------------------------------------------------------------------------------------------------
--15天聚合
-------------------------------------------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS dw_zaful_recommend.feature_items_country_v2_2_ods_info_fifteen_days (
    item_id String COMMENT '商品sku编号',
    country String COMMENT '国家',
    pv_cnt bigint COMMENT '商品的pc端曝光数',
    pv_uv bigint COMMENT '商品的pc端曝光uv',
    pv_per_cnt double COMMENT '商品的pc端人均曝光数',    
    ipv_cnt bigint COMMENT '商品的pc端点击数',
    ipv_uv bigint COMMENT '商品的pc端点击uv',
    ipv_per_cnt double COMMENT '商品的pc端人均点击数',
    bag_cnt bigint COMMENT '商品的pc端加购数',
    bag_uv bigint COMMENT '商品的pc端加购uv',
    bag_per_cnt double COMMENT '商品的pc端人均加购数',
    favorite_cnt bigint COMMENT '商品的pc端收藏数',
    favorite_uv bigint COMMENT '商品的pc端收藏uv',
    favorite_per_cnt double COMMENT '商品的pc端人均收藏数',
    order_item_cnt bigint COMMENT '商品的pc端销量(下单商品数)',
    order_uv  bigint COMMENT '商品的pc端下单uv',
    order_per_cnt double COMMENT '商品的pc端人均下单商品数',
    cvr double COMMENT '商品的pc端购买转化率',
    uv_cvr double COMMENT '商品的pc端购买uv转化率',
    order_cnt bigint COMMENT '商品的下单数',
    order_income bigint COMMENT '商品的收入',
    ctr double COMMENT '商品的点击率',
    uv_ctr double COMMENT '商品的UV点击率',
    platform  String COMMENT '平台'
    )
PARTITIONED BY (year String, month String, day String)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' LINES TERMINATED BY '\n' 
STORED AS TEXTFILE;


insert overwrite table dw_zaful_recommend.feature_items_country_v2_2_ods_info_fifteen_days partition (year='${YEAR}',month='${MONTH}',day='${DAY}')  
select 
item_id,
country,
sum(pv_cnt) as pv_cnt,
null,--count(distinct pv_uv),
null,--sum(pv_cnt)/count(distinct pv_uv) as pv_per_cnt,
sum(ipv_cnt) as ipv_cnt,
null,--count(distinct ipv_uv),
null,--sum(ipv_cnt)/count(distinct ipv_uv) as ipv_per_cnt,
sum(bag_cnt) as bag_cnt,
null,--count(distinct bag_uv) as bag_uv,
null,--sum(bag_cnt)/count(distinct bag_uv) as bag_per_cnt,
sum(favorite_cnt) as favorite_cnt,
null,--count(distinct favorite_uv) as favorite_uv,
null,--sum(favorite_cnt)/count(distinct favorite_uv) as favorite_per_cnt,
sum(order_item_cnt) as order_item_cnt,
null,--count(distinct order_uv) as order_uv,
null,--sum(order_item_cnt)/count(distinct order_uv) as order_per_cnt,
sum(order_item_cnt)/sum(ipv_cnt) as cvr,
null,--count(distinct order_uv)/count(distinct ipv_uv) as uv_cvr,
sum(order_cnt) as order_cnt,
sum(order_income) as order_income,
sum(ipv_cnt)/sum(pv_cnt) as ctr,
null,--count(distinct ipv_uv)/count(distinct pv_uv) as uv_ctr,
platform
from 
dw_zaful_recommend.feature_items_country_v2_2_ods_info
where 
concat(year,'-',month,'-',day) between date_sub('${DATE_W}',14) and '${DATE_W}'
group by item_id,platform,country
;
------------------------------------------------------------------------------------------------------------
--30天聚合
-------------------------------------------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS dw_zaful_recommend.feature_items_country_v2_2_ods_info_thirty_days (
    item_id String COMMENT '商品sku编号',
    country String COMMENT '国家',
    pv_cnt bigint COMMENT '商品的pc端曝光数',
    pv_uv bigint COMMENT '商品的pc端曝光uv',
    pv_per_cnt double COMMENT '商品的pc端人均曝光数',    
    ipv_cnt bigint COMMENT '商品的pc端点击数',
    ipv_uv bigint COMMENT '商品的pc端点击uv',
    ipv_per_cnt double COMMENT '商品的pc端人均点击数',
    bag_cnt bigint COMMENT '商品的pc端加购数',
    bag_uv bigint COMMENT '商品的pc端加购uv',
    bag_per_cnt double COMMENT '商品的pc端人均加购数',
    favorite_cnt bigint COMMENT '商品的pc端收藏数',
    favorite_uv bigint COMMENT '商品的pc端收藏uv',
    favorite_per_cnt double COMMENT '商品的pc端人均收藏数',
    order_item_cnt bigint COMMENT '商品的pc端销量(下单商品数)',
    order_uv  bigint COMMENT '商品的pc端下单uv',
    order_per_cnt double COMMENT '商品的pc端人均下单商品数',
    cvr double COMMENT '商品的pc端购买转化率',
    uv_cvr double COMMENT '商品的pc端购买uv转化率',
    order_cnt bigint COMMENT '商品的下单数',
    order_income bigint COMMENT '商品的收入',
    ctr double COMMENT '商品的点击率',
    uv_ctr double COMMENT '商品的UV点击率',
    platform  String COMMENT '平台'
    )
PARTITIONED BY (year String, month String, day String)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' LINES TERMINATED BY '\n' 
STORED AS TEXTFILE;


insert overwrite table dw_zaful_recommend.feature_items_country_v2_2_ods_info_thirty_days partition (year='${YEAR}',month='${MONTH}',day='${DAY}')  
select 
item_id,
country,
sum(pv_cnt) as pv_cnt,
null,--count(distinct pv_uv),
null,--sum(pv_cnt)/count(distinct pv_uv) as pv_per_cnt,
sum(ipv_cnt) as ipv_cnt,
null,--count(distinct ipv_uv),
null,--sum(ipv_cnt)/count(distinct ipv_uv) as ipv_per_cnt,
sum(bag_cnt) as bag_cnt,
null,--count(distinct bag_uv) as bag_uv,
null,--sum(bag_cnt)/count(distinct bag_uv) as bag_per_cnt,
sum(favorite_cnt) as favorite_cnt,
null,--count(distinct favorite_uv) as favorite_uv,
null,--sum(favorite_cnt)/count(distinct favorite_uv) as favorite_per_cnt,
sum(order_item_cnt) as order_item_cnt,
null,--count(distinct order_uv) as order_uv,
null,--sum(order_item_cnt)/count(distinct order_uv) as order_per_cnt,
sum(order_item_cnt)/sum(ipv_cnt) as cvr,
null,--count(distinct order_uv)/count(distinct ipv_uv) as uv_cvr,
sum(order_cnt) as order_cnt,
sum(order_income) as order_income,
sum(ipv_cnt)/sum(pv_cnt) as ctr,
null,--count(distinct ipv_uv)/count(distinct pv_uv) as uv_ctr,
platform
from 
dw_zaful_recommend.feature_items_country_v2_2_ods_info
where 
concat(year,'-',month,'-',day) between date_sub('${DATE_W}',29) and '${DATE_W}'
group by item_id,platform,country
;