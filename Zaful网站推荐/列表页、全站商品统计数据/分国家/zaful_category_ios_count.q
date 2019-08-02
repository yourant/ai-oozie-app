SET mapred.job.name=zaful_app_sku_cat_id_mongodb;
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









-- 所以时间均为系统时间的昨天
--订单状态0,11,13数据增量更新
--add_time=yyyy-MM-dd
insert into ai_data_analysis.order_sku_status
    select  order_sku.order_id,order_sku.goods_sn,order_sku.goods_number,order_status.order_status,order_sku.add_time
    from (
            select order_id,goods_sn,goods_number,from_unixtime(addtime,'yyyy-MM-dd') as add_time 
            from ods.ods_m_zaful_eload_order_goods 
                where from_unixtime(addtime,'yyyy-MM-dd') = '${add_time}'
            group by order_id,from_unixtime(addtime,'yyyy-MM-dd'),goods_sn,goods_number 
        ) as order_sku
    inner join
        (
            select order_id,order_status, from_unixtime(add_time,'yyyy-MM-dd') as add_time 
            from ods.ods_m_zaful_eload_order_info 
                where from_unixtime(add_time,'yyyy-MM-dd') = '${add_time}' and order_status in(0,11,13)
            group by order_id,order_status, from_unixtime(add_time,'yyyy-MM-dd')
        ) as order_status
        on order_sku.order_id=order_status.order_id and order_sku.add_time=order_status.add_time
;


--Zaful商品信息最新一次更新
--dt=yyyyMMdd
set hive.support.concurrency=false;
insert overwrite table ai_data_analysis.zaful_sku_info_last
    select goods_sn,shop_price,is_best,is_new,is_hot,is_free_shipping,is_alone_sale,is_promote,
            is_home,is_login,is_recommend,is_direct_sale_off,week2sale,week1sale,up,dt,
            ROW_NUMBER() OVER(PARTITION BY goods_sn ORDER BY dt desc) as num 
        from ods.ods_m_zaful_eload_goods
        where dt = '${dt}'
;



--订单状态0,11,13统计数据(train)
set hive.support.concurrency=false;
insert overwrite table ai_data_analysis.order_sku_status_count_train
-- create table ai_data_analysis.order_sku_status_count_train
-- as
select a.goods_sn,a.goods_num,
    order_status0_num30,order_status11_num30,order_status13_num30,
    order_status0_num15,order_status11_num15,order_status13_num15,
    order_status0_num10,order_status11_num10,order_status13_num10,
    order_status0_num7,order_status11_num7,order_status13_num7,
    order_status0_num3,order_status11_num3,order_status13_num3,
    order_status0_num1,order_status11_num1,order_status13_num1,
    order_status0_num,order_status11_num,order_status13_num,
    order_status0_num_f,order_status11_num_f,order_status13_num_f
from(select goods_sn,sum(goods_number) as goods_num,
    count(case when order_status=0 then goods_sn else null end)  order_status0_num30,
    count(case when order_status=11 then goods_sn else null end) order_status11_num30,
    count(case when order_status=13 then goods_sn else null end) order_status13_num30
    from ai_data_analysis.order_sku_status
    where add_time between date_add('${add_time}',-31) and date_add('${add_time}',-2) group by goods_sn) a
left join (select goods_sn,
    count(case when order_status=0 then goods_sn else null end)  order_status0_num15,
    count(case when order_status=11 then goods_sn else null end) order_status11_num15,
    count(case when order_status=13 then goods_sn else null end) order_status13_num15
    from ai_data_analysis.order_sku_status
    where add_time between date_add('${add_time}',-16) and date_add('${add_time}',-2) group by goods_sn) b
    on a.goods_sn = b.goods_sn
left join (select goods_sn,
    count(case when order_status=0 then goods_sn else null end)  order_status0_num10,
    count(case when order_status=11 then goods_sn else null end) order_status11_num10,
    count(case when order_status=13 then goods_sn else null end) order_status13_num10
    from ai_data_analysis.order_sku_status
    where add_time between date_add('${add_time}',-11) and date_add('${add_time}',-2) group by goods_sn) c
    on a.goods_sn = c.goods_sn 
left join (select goods_sn,
    count(case when order_status=0 then goods_sn else null end)  order_status0_num7,
    count(case when order_status=11 then goods_sn else null end) order_status11_num7,
    count(case when order_status=13 then goods_sn else null end) order_status13_num7
    from ai_data_analysis.order_sku_status
    where add_time between date_add('${add_time}',-8) and date_add('${add_time}',-2) group by goods_sn) d
    on a.goods_sn = d.goods_sn 
left join (select goods_sn,
    count(case when order_status=0 then goods_sn else null end)  order_status0_num3,
    count(case when order_status=11 then goods_sn else null end) order_status11_num3,
    count(case when order_status=13 then goods_sn else null end) order_status13_num3
    from ai_data_analysis.order_sku_status
    where add_time between date_add('${add_time}',-4) and date_add('${add_time}',-2) group by goods_sn) e
    on a.goods_sn = e.goods_sn
left join (select goods_sn,
    count(case when order_status=0 then goods_sn else null end)  order_status0_num1,
    count(case when order_status=11 then goods_sn else null end) order_status11_num1,
    count(case when order_status=13 then goods_sn else null end) order_status13_num1
    from ai_data_analysis.order_sku_status
    where add_time = date_add('${add_time}',-2) group by goods_sn) f
    on a.goods_sn = f.goods_sn
left join (select goods_sn,
    count(case when order_status=0 then goods_sn else null end)  order_status0_num,
    count(case when order_status=11 then goods_sn else null end) order_status11_num,
    count(case when order_status=13 then goods_sn else null end) order_status13_num
    from ai_data_analysis.order_sku_status
    where add_time = date_add('${add_time}',-1) group by goods_sn) g
    on a.goods_sn = g.goods_sn
left join (select goods_sn,
    count(case when order_status=0 then goods_sn else null end)  order_status0_num_f,
    count(case when order_status=11 then goods_sn else null end) order_status11_num_f,
    count(case when order_status=13 then goods_sn else null end) order_status13_num_f
    from ai_data_analysis.order_sku_status
    where add_time = '${add_time}' group by goods_sn) h
    on a.goods_sn = h.goods_sn
;
--zaful app 全量sku行为数据
--add_time=yyyy-MM-dd
set hive.support.concurrency=false;
insert overwrite table ai_data_analysis.zaful_list_rank_count_train
-- create table ai_data_analysis.zaful_list_rank_count_train
-- as
select a.sku,a.cat_id,
        count30,exp_num30,click_num30,adf_num30,adt_user_num30,adt_num30,order_user_num30,order_num30,pay_user_num30,pay_num30,gmv_user_num30,gmv_num30,sale_user_num30,sale_num30,ctr_30,tcvr_30,
        count15,exp_num15,click_num15,adf_num15,adt_user_num15,adt_num15,order_user_num15,order_num15,pay_user_num15,pay_num15,gmv_user_num15,gmv_num15,sale_user_num15,sale_num15,ctr_15,tcvr_15,
        count10,exp_num10,click_num10,adf_num10,adt_user_num10,adt_num10,order_user_num10,order_num10,pay_user_num10,pay_num10,gmv_user_num10,gmv_num10,sale_user_num10,sale_num10,ctr_10,tcvr_10,
        count7,exp_num7,click_num7,adf_num7,adt_user_num7,adt_num7,order_user_num7,order_num7,pay_user_num7,pay_num7,gmv_user_num7,gmv_num7,sale_user_num7,sale_num7,ctr_7,tcvr_7,
        count3,exp_num3,click_num3,adf_num3,adt_user_num3,adt_num3,order_user_num3,order_num3,pay_user_num3,pay_num3,gmv_user_num3,gmv_num3,sale_user_num3,sale_num3,ctr_3,tcvr_3,
        count1,exp_num1,click_num1,adf_num1,adt_user_num1,adt_num1,order_user_num1,order_num1,pay_user_num1,pay_num1,gmv_user_num1,gmv_num1,sale_user_num1,sale_num1,ctr_1,tcvr_1,
        order_user_num,order_num,pay_user_num,pay_num,gmv_user_num,gmv_num,sale_user_num,sale_num,
        order_user_num_f,order_num_f,pay_user_num_f,pay_num_f,gmv_user_num_f,gmv_num_f,sale_user_num_f,sale_num_f
from(select sku,cat_id,count(distinct date,platform) as count30,sum(af_impression) as exp_num30,sum( af_view_product) as click_num30,sum(af_add_to_wishlist) as adf_num30,sum(af_add_to_bag_user) as adt_user_num30,
    sum(af_add_to_bag_num) as adt_num30,sum(af_create_order_success_user) as order_user_num30,sum(af_create_order_success_num) as order_num30,sum(af_purchase_user) as pay_user_num30,
    sum(af_purchase_num) as pay_num30,sum(gmv_user) as gmv_user_num30,sum(gmv_num) as gmv_num30,sum(sale_value_user) as sale_user_num30,sum(sale_value_num) as sale_num30,
    (sum(af_view_product)/sum(af_impression)*100) as ctr_30,(sum(af_create_order_success_num)/sum(af_view_product)*100) as tcvr_30
    from dw_proj.app_zaful_sku_cat_id_no_country_all_wuc_wishlist 
    where from_unixtime(unix_timestamp(add_time,'yyyyMMdd'),'yyyy-MM-dd') between date_add('${add_time}',-31) and date_add('${add_time}',-2) and cat_id is not null group by sku,cat_id) a 
left join (select sku,cat_id,count(distinct date,platform) as count15,sum(af_impression) as exp_num15,sum( af_view_product) as click_num15,sum(af_add_to_wishlist) as adf_num15,sum(af_add_to_bag_user) as adt_user_num15,
    sum(af_add_to_bag_num) as adt_num15,sum(af_create_order_success_user) as order_user_num15,sum(af_create_order_success_num) as order_num15,sum(af_purchase_user) as pay_user_num15,
    sum(af_purchase_num) as pay_num15,sum(gmv_user) as gmv_user_num15,sum(gmv_num) as gmv_num15,sum(sale_value_user) as sale_user_num15,sum(sale_value_num) as sale_num15,
    (sum(af_view_product)/sum(af_impression)*100) as ctr_15,(sum(af_create_order_success_num)/sum(af_view_product)*100) as tcvr_15
    from dw_proj.app_zaful_sku_cat_id_no_country_all_wuc_wishlist 
    where from_unixtime(unix_timestamp(add_time,'yyyyMMdd'),'yyyy-MM-dd') between date_add('${add_time}',-16) and date_add('${add_time}',-2) and cat_id is not null group by sku,cat_id) b
    on a.cat_id = b.cat_id and a.sku = b.sku
left join (select sku,cat_id,count(distinct date,platform) as count10,sum(af_impression) as exp_num10,sum( af_view_product) as click_num10,sum(af_add_to_wishlist) as adf_num10,sum(af_add_to_bag_user) as adt_user_num10,
    sum(af_add_to_bag_num) as adt_num10,sum(af_create_order_success_user) as order_user_num10,sum(af_create_order_success_num) as order_num10,sum(af_purchase_user) as pay_user_num10,
    sum(af_purchase_num) as pay_num10,sum(gmv_user) as gmv_user_num10,sum(gmv_num) as gmv_num10,sum(sale_value_user) as sale_user_num10,sum(sale_value_num) as sale_num10,
    (sum(af_view_product)/sum(af_impression)*100) as ctr_10,(sum(af_create_order_success_num)/sum(af_view_product)*100) as tcvr_10
    from dw_proj.app_zaful_sku_cat_id_no_country_all_wuc_wishlist 
    where from_unixtime(unix_timestamp(add_time,'yyyyMMdd'),'yyyy-MM-dd') between date_add('${add_time}',-11) and date_add('${add_time}',-2) and cat_id is not null group by sku,cat_id) c
    on a.cat_id = c.cat_id and a.sku = c.sku
left join (select sku,cat_id,count(distinct date,platform) as count7,sum(af_impression) as exp_num7,sum( af_view_product) as click_num7,sum(af_add_to_wishlist) as adf_num7,sum(af_add_to_bag_user) as adt_user_num7,
    sum(af_add_to_bag_num) as adt_num7,sum(af_create_order_success_user) as order_user_num7,sum(af_create_order_success_num) as order_num7,sum(af_purchase_user) as pay_user_num7,
    sum(af_purchase_num) as pay_num7,sum(gmv_user) as gmv_user_num7,sum(gmv_num) as gmv_num7,sum(sale_value_user) as sale_user_num7,sum(sale_value_num) as sale_num7,
    (sum(af_view_product)/sum(af_impression)*100) as ctr_7,(sum(af_create_order_success_num)/sum(af_view_product)*100) as tcvr_7
    from dw_proj.app_zaful_sku_cat_id_no_country_all_wuc_wishlist 
    where from_unixtime(unix_timestamp(add_time,'yyyyMMdd'),'yyyy-MM-dd') between date_add('${add_time}',-8) and date_add('${add_time}',-2) and cat_id is not null group by sku,cat_id) d
    on a.cat_id = d.cat_id and a.sku = d.sku
left join (select sku,cat_id,count(distinct date,platform) as count3,sum(af_impression) as exp_num3,sum( af_view_product) as click_num3,sum(af_add_to_wishlist) as adf_num3,sum(af_add_to_bag_user) as adt_user_num3,
    sum(af_add_to_bag_num) as adt_num3,sum(af_create_order_success_user) as order_user_num3,sum(af_create_order_success_num) as order_num3,sum(af_purchase_user) as pay_user_num3,
    sum(af_purchase_num) as pay_num3,sum(gmv_user) as gmv_user_num3,sum(gmv_num) as gmv_num3,sum(sale_value_user) as sale_user_num3,sum(sale_value_num) as sale_num3,
    (sum(af_view_product)/sum(af_impression)*100) as ctr_3,(sum(af_create_order_success_num)/sum(af_view_product)*100) as tcvr_3
    from dw_proj.app_zaful_sku_cat_id_no_country_all_wuc_wishlist 
    where from_unixtime(unix_timestamp(add_time,'yyyyMMdd'),'yyyy-MM-dd') between date_add('${add_time}',-4) and date_add('${add_time}',-2) and cat_id is not null group by sku,cat_id) e
    on a.cat_id = e.cat_id and a.sku = e.sku
left join (select sku,cat_id,count(distinct date,platform) as count1,sum(af_impression) as exp_num1,sum( af_view_product) as click_num1,sum(af_add_to_wishlist) as adf_num1,sum(af_add_to_bag_user) as adt_user_num1,
    sum(af_add_to_bag_num) as adt_num1,sum(af_create_order_success_user) as order_user_num1,sum(af_create_order_success_num) as order_num1,sum(af_purchase_user) as pay_user_num1,
    sum(af_purchase_num) as pay_num1,sum(gmv_user) as gmv_user_num1,sum(gmv_num) as gmv_num1,sum(sale_value_user) as sale_user_num1,sum(sale_value_num) as sale_num1,
    (sum(af_view_product)/sum(af_impression)*100) as ctr_1,(sum(af_create_order_success_num)/sum(af_view_product)*100) as tcvr_1
    from dw_proj.app_zaful_sku_cat_id_no_country_all_wuc_wishlist 
    where from_unixtime(unix_timestamp(add_time,'yyyyMMdd'),'yyyy-MM-dd') = date_add('${add_time}',-2) and cat_id is not null group by sku,cat_id) f
    on a.cat_id = f.cat_id and a.sku = f.sku
left join (select sku,cat_id,sum(af_create_order_success_user) as order_user_num,sum(af_create_order_success_num) as order_num,sum(af_purchase_user) as pay_user_num,
    sum(af_purchase_num) as pay_num,sum(gmv_user) as gmv_user_num,sum(gmv_num) as gmv_num,sum(sale_value_user) as sale_user_num,sum(sale_value_num) as sale_num
    from dw_proj.app_zaful_sku_cat_id_no_country_all_wuc_wishlist 
    where from_unixtime(unix_timestamp(add_time,'yyyyMMdd'),'yyyy-MM-dd') = date_add('${add_time}',-1) and cat_id is not null  group by sku,cat_id) as g
    on a.cat_id = g.cat_id and a.sku = g.sku
left join (select sku,cat_id,sum(af_create_order_success_user) as order_user_num_f,sum(af_create_order_success_num) as order_num_f,sum(af_purchase_user) as pay_user_num_f,
    sum(af_purchase_num) as pay_num_f,sum(gmv_user) as gmv_user_num_f,sum(gmv_num) as gmv_num_f,sum(sale_value_user) as sale_user_num_f,sum(sale_value_num) as sale_num_f
    from dw_proj.app_zaful_sku_cat_id_no_country_all_wuc_wishlist 
    where from_unixtime(unix_timestamp(add_time,'yyyyMMdd'),'yyyy-MM-dd') = '${add_time}' and cat_id is not null group by sku,cat_id) as h
    on a.cat_id = h.cat_id and a.sku = h.sku
;


--zaful 类目页排序训练集
set hive.support.concurrency=false;
insert overwrite table ai_data_analysis.zaful_list_rank_train
-- create table ai_data_analysis.zaful_list_rank_train
-- as
select aa.sku,cat_id,goods_num,
        count30,exp_num30,click_num30,adf_num30,adt_user_num30,adt_num30,order_user_num30,order_num30,pay_user_num30,pay_num30,gmv_user_num30,gmv_num30,sale_user_num30,sale_num30,ctr_30,tcvr_30,
        order_status0_num30,order_status11_num30,order_status13_num30,
        count15,exp_num15,click_num15,adf_num15,adt_user_num15,adt_num15,order_user_num15,order_num15,pay_user_num15,pay_num15,gmv_user_num15,gmv_num15,sale_user_num15,sale_num15,ctr_15,tcvr_15,
        order_status0_num15,order_status11_num15,order_status13_num15,
        count10,exp_num10,click_num10,adf_num10,adt_user_num10,adt_num10,order_user_num10,order_num10,pay_user_num10,pay_num10,gmv_user_num10,gmv_num10,sale_user_num10,sale_num10,ctr_10,tcvr_10,
        order_status0_num10,order_status11_num10,order_status13_num10,
        count7,exp_num7,click_num7,adf_num7,adt_user_num7,adt_num7,order_user_num7,order_num7,pay_user_num7,pay_num7,gmv_user_num7,gmv_num7,sale_user_num7,sale_num7,ctr_7,tcvr_7,
        order_status0_num7,order_status11_num7,order_status13_num7,
        count3,exp_num3,click_num3,adf_num3,adt_user_num3,adt_num3,order_user_num3,order_num3,pay_user_num3,pay_num3,gmv_user_num3,gmv_num3,sale_user_num3,sale_num3,ctr_3,tcvr_3,
        order_status0_num3,order_status11_num3,order_status13_num3,
        count1,exp_num1,click_num1,adf_num1,adt_user_num1,adt_num1,order_user_num1,order_num1,pay_user_num1,pay_num1,gmv_user_num1,gmv_num1,sale_user_num1,sale_num1,ctr_1,tcvr_1,
        order_status0_num1,order_status11_num1,order_status13_num1,
        order_user_num,order_num,pay_user_num,pay_num,gmv_user_num,gmv_num,sale_user_num,sale_num,
        order_status0_num,order_status11_num,order_status13_num,
        order_user_num_f,order_num_f,pay_user_num_f,pay_num_f,gmv_user_num_f,gmv_num_f,sale_user_num_f,sale_num_f,
        order_status0_num_f,order_status11_num_f,order_status13_num_f,
        shop_price,is_best,is_new,is_hot,is_free_shipping,is_alone_sale,is_promote,
        is_home,is_login,is_recommend,is_direct_sale_off,week2sale,week1sale,up
from(select * from ai_data_analysis.zaful_list_rank_count_train) aa
left join(select * from ai_data_analysis.order_sku_status_count_train) bb 
            on aa.sku = bb.goods_sn
left join(select * from ai_data_analysis.zaful_sku_info_last) cc
            on aa.sku = cc.goods_sn
;

--订单状态0,11,13统计数据(verify)
set hive.support.concurrency=false;
insert overwrite table ai_data_analysis.order_sku_status_count_verify
-- create table ai_data_analysis.order_sku_status_count_verify
-- as
select a.goods_sn,a.goods_num,
    order_status0_num30,order_status11_num30,order_status13_num30,
    order_status0_num15,order_status11_num15,order_status13_num15,
    order_status0_num10,order_status11_num10,order_status13_num10,
    order_status0_num7,order_status11_num7,order_status13_num7,
    order_status0_num3,order_status11_num3,order_status13_num3,
    order_status0_num1,order_status11_num1,order_status13_num1,
    order_status0_num,order_status11_num,order_status13_num
from(select goods_sn,sum(goods_number) as goods_num,
    count(case when order_status=0 then goods_sn else null end)  order_status0_num30,
    count(case when order_status=11 then goods_sn else null end) order_status11_num30,
    count(case when order_status=13 then goods_sn else null end) order_status13_num30
    from ai_data_analysis.order_sku_status
    where add_time between date_add('${add_time}',-30) and date_add('${add_time}',-1) group by goods_sn) a
left join (select goods_sn,
    count(case when order_status=0 then goods_sn else null end)  order_status0_num15,
    count(case when order_status=11 then goods_sn else null end) order_status11_num15,
    count(case when order_status=13 then goods_sn else null end) order_status13_num15
    from ai_data_analysis.order_sku_status
    where add_time between date_add('${add_time}',-15) and date_add('${add_time}',-1) group by goods_sn) b
    on a.goods_sn = b.goods_sn
left join (select goods_sn,
    count(case when order_status=0 then goods_sn else null end)  order_status0_num10,
    count(case when order_status=11 then goods_sn else null end) order_status11_num10,
    count(case when order_status=13 then goods_sn else null end) order_status13_num10
    from ai_data_analysis.order_sku_status
    where add_time between date_add('${add_time}',-10) and date_add('${add_time}',-1) group by goods_sn) c
    on a.goods_sn = c.goods_sn
left join (select goods_sn,
    count(case when order_status=0 then goods_sn else null end)  order_status0_num7,
    count(case when order_status=11 then goods_sn else null end) order_status11_num7,
    count(case when order_status=13 then goods_sn else null end) order_status13_num7
    from ai_data_analysis.order_sku_status
    where add_time between date_add('${add_time}',-7) and date_add('${add_time}',-1) group by goods_sn) d
    on a.goods_sn = d.goods_sn
left join (select goods_sn,
    count(case when order_status=0 then goods_sn else null end)  order_status0_num3,
    count(case when order_status=11 then goods_sn else null end) order_status11_num3,
    count(case when order_status=13 then goods_sn else null end) order_status13_num3
    from ai_data_analysis.order_sku_status
    where add_time between date_add('${add_time}',-3) and date_add('${add_time}',-1) group by goods_sn) e
    on a.goods_sn = e.goods_sn
left join (select goods_sn,
    count(case when order_status=0 then goods_sn else null end)  order_status0_num1,
    count(case when order_status=11 then goods_sn else null end) order_status11_num1,
    count(case when order_status=13 then goods_sn else null end) order_status13_num1
    from ai_data_analysis.order_sku_status
    where add_time = date_add('${add_time}',-1) group by goods_sn) f
    on a.goods_sn = f.goods_sn
left join (select goods_sn,
    count(case when order_status=0 then goods_sn else null end)  order_status0_num,
    count(case when order_status=11 then goods_sn else null end) order_status11_num,
    count(case when order_status=13 then goods_sn else null end) order_status13_num
    from ai_data_analysis.order_sku_status
    where add_time = '${add_time}' group by goods_sn) g
    on a.goods_sn = g.goods_sn
;

--zaful app 全量sku行为数据
--add_time=yyyy-MM-dd
set hive.support.concurrency=false;
insert overwrite table ai_data_analysis.zaful_list_rank_count_verify
-- create table ai_data_analysis.zaful_list_rank_count_verify
-- as
select a.sku,a.cat_id,
        count30,exp_num30,click_num30,adf_num30,adt_user_num30,adt_num30,order_user_num30,order_num30,pay_user_num30,pay_num30,gmv_user_num30,gmv_num30,sale_user_num30,sale_num30,ctr_30,tcvr_30,
        count15,exp_num15,click_num15,adf_num15,adt_user_num15,adt_num15,order_user_num15,order_num15,pay_user_num15,pay_num15,gmv_user_num15,gmv_num15,sale_user_num15,sale_num15,ctr_15,tcvr_15,
        count10,exp_num10,click_num10,adf_num10,adt_user_num10,adt_num10,order_user_num10,order_num10,pay_user_num10,pay_num10,gmv_user_num10,gmv_num10,sale_user_num10,sale_num10,ctr_10,tcvr_10,
        count7,exp_num7,click_num7,adf_num7,adt_user_num7,adt_num7,order_user_num7,order_num7,pay_user_num7,pay_num7,gmv_user_num7,gmv_num7,sale_user_num7,sale_num7,ctr_7,tcvr_7,
        count3,exp_num3,click_num3,adf_num3,adt_user_num3,adt_num3,order_user_num3,order_num3,pay_user_num3,pay_num3,gmv_user_num3,gmv_num3,sale_user_num3,sale_num3,ctr_3,tcvr_3,
        count1,exp_num1,click_num1,adf_num1,adt_user_num1,adt_num1,order_user_num1,order_num1,pay_user_num1,pay_num1,gmv_user_num1,gmv_num1,sale_user_num1,sale_num1,ctr_1,tcvr_1,
        order_user_num,order_num,pay_user_num,pay_num,gmv_user_num,gmv_num,sale_user_num,sale_num
from(select sku,cat_id,count(distinct date,platform) as count30,sum(af_impression) as exp_num30,sum( af_view_product) as click_num30,sum(af_add_to_wishlist) as adf_num30,sum(af_add_to_bag_user) as adt_user_num30,
    sum(af_add_to_bag_num) as adt_num30,sum(af_create_order_success_user) as order_user_num30,sum(af_create_order_success_num) as order_num30,sum(af_purchase_user) as pay_user_num30,
    sum(af_purchase_num) as pay_num30,sum(gmv_user) as gmv_user_num30,sum(gmv_num) as gmv_num30,sum(sale_value_user) as sale_user_num30,sum(sale_value_num) as sale_num30,
    (sum(af_view_product)/sum(af_impression)*100) as ctr_30,(sum(af_create_order_success_num)/sum(af_view_product)*100) as tcvr_30
    from dw_proj.app_zaful_sku_cat_id_no_country_all_wuc_wishlist 
    where from_unixtime(unix_timestamp(add_time,'yyyyMMdd'),'yyyy-MM-dd') between date_add('${add_time}',-30) and date_add('${add_time}',-1) and cat_id is not null group by sku,cat_id) a 
left join (select sku,cat_id,count(distinct date,platform) as count15,sum(af_impression) as exp_num15,sum( af_view_product) as click_num15,sum(af_add_to_wishlist) as adf_num15,sum(af_add_to_bag_user) as adt_user_num15,
    sum(af_add_to_bag_num) as adt_num15,sum(af_create_order_success_user) as order_user_num15,sum(af_create_order_success_num) as order_num15,sum(af_purchase_user) as pay_user_num15,
    sum(af_purchase_num) as pay_num15,sum(gmv_user) as gmv_user_num15,sum(gmv_num) as gmv_num15,sum(sale_value_user) as sale_user_num15,sum(sale_value_num) as sale_num15,
    (sum(af_view_product)/sum(af_impression)*100) as ctr_15,(sum(af_create_order_success_num)/sum(af_view_product)*100) as tcvr_15
    from dw_proj.app_zaful_sku_cat_id_no_country_all_wuc_wishlist 
    where from_unixtime(unix_timestamp(add_time,'yyyyMMdd'),'yyyy-MM-dd') between date_add('${add_time}',-15) and date_add('${add_time}',-1) and cat_id is not null group by sku,cat_id) b 
    on a.cat_id = b.cat_id and a.sku = b.sku
left join (select sku,cat_id,count(distinct date,platform) as count10,sum(af_impression) as exp_num10,sum( af_view_product) as click_num10,sum(af_add_to_wishlist) as adf_num10,sum(af_add_to_bag_user) as adt_user_num10,
    sum(af_add_to_bag_num) as adt_num10,sum(af_create_order_success_user) as order_user_num10,sum(af_create_order_success_num) as order_num10,sum(af_purchase_user) as pay_user_num10,
    sum(af_purchase_num) as pay_num10,sum(gmv_user) as gmv_user_num10,sum(gmv_num) as gmv_num10,sum(sale_value_user) as sale_user_num10,sum(sale_value_num) as sale_num10,
    (sum(af_view_product)/sum(af_impression)*100) as ctr_10,(sum(af_create_order_success_num)/sum(af_view_product)*100) as tcvr_10
    from dw_proj.app_zaful_sku_cat_id_no_country_all_wuc_wishlist 
    where from_unixtime(unix_timestamp(add_time,'yyyyMMdd'),'yyyy-MM-dd') between date_add('${add_time}',-10) and date_add('${add_time}',-1) and cat_id is not null group by sku,cat_id) c 
    on a.cat_id = c.cat_id and a.sku = c.sku
left join (select sku,cat_id,count(distinct date,platform) as count7,sum(af_impression) as exp_num7,sum( af_view_product) as click_num7,sum(af_add_to_wishlist) as adf_num7,sum(af_add_to_bag_user) as adt_user_num7,
    sum(af_add_to_bag_num) as adt_num7,sum(af_create_order_success_user) as order_user_num7,sum(af_create_order_success_num) as order_num7,sum(af_purchase_user) as pay_user_num7,
    sum(af_purchase_num) as pay_num7,sum(gmv_user) as gmv_user_num7,sum(gmv_num) as gmv_num7,sum(sale_value_user) as sale_user_num7,sum(sale_value_num) as sale_num7,
    (sum(af_view_product)/sum(af_impression)*100) as ctr_7,(sum(af_create_order_success_num)/sum(af_view_product)*100) as tcvr_7
    from dw_proj.app_zaful_sku_cat_id_no_country_all_wuc_wishlist 
    where from_unixtime(unix_timestamp(add_time,'yyyyMMdd'),'yyyy-MM-dd') between date_add('${add_time}',-7) and date_add('${add_time}',-1) and cat_id is not null group by sku,cat_id) d 
    on a.cat_id = d.cat_id and a.sku = d.sku
left join (select sku,cat_id,count(distinct date,platform) as count3,sum(af_impression) as exp_num3,sum( af_view_product) as click_num3,sum(af_add_to_wishlist) as adf_num3,sum(af_add_to_bag_user) as adt_user_num3,
    sum(af_add_to_bag_num) as adt_num3,sum(af_create_order_success_user) as order_user_num3,sum(af_create_order_success_num) as order_num3,sum(af_purchase_user) as pay_user_num3,
    sum(af_purchase_num) as pay_num3,sum(gmv_user) as gmv_user_num3,sum(gmv_num) as gmv_num3,sum(sale_value_user) as sale_user_num3,sum(sale_value_num) as sale_num3,
    (sum(af_view_product)/sum(af_impression)*100) as ctr_3,(sum(af_create_order_success_num)/sum(af_view_product)*100) as tcvr_3
    from dw_proj.app_zaful_sku_cat_id_no_country_all_wuc_wishlist 
    where from_unixtime(unix_timestamp(add_time,'yyyyMMdd'),'yyyy-MM-dd') between date_add('${add_time}',-3) and date_add('${add_time}',-1) and cat_id is not null group by sku,cat_id) e 
    on a.cat_id = e.cat_id and a.sku = e.sku
left join (select sku,cat_id,count(distinct date,platform) as count1,sum(af_impression) as exp_num1,sum( af_view_product) as click_num1,sum(af_add_to_wishlist) as adf_num1,sum(af_add_to_bag_user) as adt_user_num1,
    sum(af_add_to_bag_num) as adt_num1,sum(af_create_order_success_user) as order_user_num1,sum(af_create_order_success_num) as order_num1,sum(af_purchase_user) as pay_user_num1,
    sum(af_purchase_num) as pay_num1,sum(gmv_user) as gmv_user_num1,sum(gmv_num) as gmv_num1,sum(sale_value_user) as sale_user_num1,sum(sale_value_num) as sale_num1,
    (sum(af_view_product)/sum(af_impression)*100) as ctr_1,(sum(af_create_order_success_num)/sum(af_view_product)*100) as tcvr_1
    from dw_proj.app_zaful_sku_cat_id_no_country_all_wuc_wishlist 
    where from_unixtime(unix_timestamp(add_time,'yyyyMMdd'),'yyyy-MM-dd') = date_add('${add_time}',-1) and cat_id is not null group by sku,cat_id) f 
    on a.cat_id = f.cat_id and a.sku = f.sku
left join (select sku,cat_id,sum(af_create_order_success_user) as order_user_num,sum(af_create_order_success_num) as order_num,sum(af_purchase_user) as pay_user_num,
    sum(af_purchase_num) as pay_num,sum(gmv_user) as gmv_user_num,sum(gmv_num) as gmv_num,sum(sale_value_user) as sale_user_num,sum(sale_value_num) as sale_num
    from dw_proj.app_zaful_sku_cat_id_no_country_all_wuc_wishlist 
    where from_unixtime(unix_timestamp(add_time,'yyyyMMdd'),'yyyy-MM-dd') = '${add_time}' and cat_id is not null  group by sku,cat_id) as g 
    on a.cat_id = g.cat_id and a.sku = g.sku
;



--zaful 类目页排序验证集
set hive.support.concurrency=false;
insert overwrite table ai_data_analysis.zaful_list_rank_verify
-- create table ai_data_analysis.zaful_list_rank_verify
-- as
select aa.sku,cat_id,goods_num,
        count30,exp_num30,click_num30,adf_num30,adt_user_num30,adt_num30,order_user_num30,order_num30,pay_user_num30,pay_num30,gmv_user_num30,gmv_num30,sale_user_num30,sale_num30,ctr_30,tcvr_30,
        order_status0_num30,order_status11_num30,order_status13_num30,
        count15,exp_num15,click_num15,adf_num15,adt_user_num15,adt_num15,order_user_num15,order_num15,pay_user_num15,pay_num15,gmv_user_num15,gmv_num15,sale_user_num15,sale_num15,ctr_15,tcvr_15,
        order_status0_num15,order_status11_num15,order_status13_num15,
        count10,exp_num10,click_num10,adf_num10,adt_user_num10,adt_num10,order_user_num10,order_num10,pay_user_num10,pay_num10,gmv_user_num10,gmv_num10,sale_user_num10,sale_num10,ctr_10,tcvr_10,
        order_status0_num10,order_status11_num10,order_status13_num10,
        count7,exp_num7,click_num7,adf_num7,adt_user_num7,adt_num7,order_user_num7,order_num7,pay_user_num7,pay_num7,gmv_user_num7,gmv_num7,sale_user_num7,sale_num7,ctr_7,tcvr_7,
        order_status0_num7,order_status11_num7,order_status13_num7,
        count3,exp_num3,click_num3,adf_num3,adt_user_num3,adt_num3,order_user_num3,order_num3,pay_user_num3,pay_num3,gmv_user_num3,gmv_num3,sale_user_num3,sale_num3,ctr_3,tcvr_3,
        order_status0_num3,order_status11_num3,order_status13_num3,
        count1,exp_num1,click_num1,adf_num1,adt_user_num1,adt_num1,order_user_num1,order_num1,pay_user_num1,pay_num1,gmv_user_num1,gmv_num1,sale_user_num1,sale_num1,ctr_1,tcvr_1,
        order_status0_num1,order_status11_num1,order_status13_num1,
        order_user_num,order_num,pay_user_num,pay_num,gmv_user_num,gmv_num,sale_user_num,sale_num,
        order_status0_num,order_status11_num,order_status13_num,
        shop_price,is_best,is_new,is_hot,is_free_shipping,is_alone_sale,is_promote,
        is_home,is_login,is_recommend,is_direct_sale_off,week2sale,week1sale,up
from(select * from ai_data_analysis.zaful_list_rank_count_verify) aa
left join(select * from ai_data_analysis.order_sku_status_count_verify) bb 
            on aa.sku = bb.goods_sn
left join(select * from ai_data_analysis.zaful_sku_info_last) cc
            on aa.sku = cc.goods_sn
;


insert OVERWRITE table  dw_zaful_recommend.zaful_category_ios_count_confirm PARTITION (date = '${add_time}')
select
'${add_time}'
;








