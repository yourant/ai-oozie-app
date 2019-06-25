
--@author wuchao
--@date 2019年02月12日 
--@desc  王忠付大禹数据迁移


SET mapred.job.name=zaful_app_ods_ams;
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


--  create table if not exists dw_gearbest_recommend.gb_app_abset_id_user_fb(
--  adset_id                       STRING    comment 'device_id',
--  user_fb_age_range              STRING    comment 'FB年龄段',
--  user_fb_banner_click           STRING    comment 'FB广告点击位置',
--  user_fb_platform               STRING    comment 'FB展示平台',
--  user_fb_interests              STRING    comment 'FB兴趣',       
--  user_fb_genders                STRING    comment 'FB性别'
--  )
--  PARTITIONED BY (add_time STRING)
--  ROW FORMAT DELIMITED FIELDS TERMINATED BY '\u0001' LINES TERMINATED BY '\n' STORED AS TEXTFILE
--  ;

 create table if not exists dw_zaful_recommend.zaful_app_abset_id_user_fb_all(
 adset_id                       STRING    comment 'fb广告id',
 user_fb_all             STRING    comment '全部广告信息'
 )
 ROW FORMAT DELIMITED FIELDS TERMINATED BY '\u0001' LINES TERMINATED BY '\n' STORED AS TEXTFILE
 ;

insert OVERWRITE table dw_zaful_recommend.zaful_app_abset_id_user_fb_all 
select 
b.adset_id,
concat('user_fb_age_range','=',b.user_fb_age_range,'||','user_fb_banner_click','=',b.user_fb_banner_click,'||','user_fb_platform','=',b.user_fb_platform,'||','user_fb_interests','=',b.user_fb_interests,'||','user_fb_genders','=',b.user_fb_genders)
from
(
    select 
    x.adset_id,
    concat(nvl(z.fieldvalue,''),'-',nvl(y.fieldvalue,'')) as user_fb_age_range,
    nvl(o.fieldvalue,'') as user_fb_banner_click,
    nvl(p.fieldvalue,'') as user_fb_platform,
    nvl(r.user_fb_interests,'') as user_fb_interests,
    nvl(q.fieldvalue,'') as user_fb_genders
    from 
        (
            select 
            adset_id
            from
            ods.ods_s_ams_ams_test_tb_adset_details
            group by adset_id
        ) x
        left join 
        (
            select 
            a.id,
            a.adset_id,
            a.fieldname,
            a.fieldvalue,
            a.num
            from 
            (
                select 
                id,
                adset_id,
                fieldname,
                fieldvalue,
                row_number() over ( partition by adset_id order by id desc  ) num 
                from
                ods.ods_s_ams_ams_test_tb_adset_details
                where
                fieldname in ('age_max')
                group by adset_id,fieldname,fieldvalue,id
            ) a 
            where a.num=1
        ) y
        on x.adset_id=y.adset_id

        left join 
        (
            select 
            a.id,
            a.adset_id,
            a.fieldname,
            a.fieldvalue,
            a.num
            from 
            (
                select 
                id,
                adset_id,
                fieldname,
                fieldvalue,
                row_number() over ( partition by adset_id order by id desc  ) num 
                from
                ods.ods_s_ams_ams_test_tb_adset_details
                where
                fieldname in ('age_min')
                group by adset_id,fieldname,fieldvalue,id
            ) a 
            where a.num=1
        ) z
        on x.adset_id=z.adset_id

        left join 
        (
            select 
            a.adset_id,
            concat_ws(',',collect_set(nvl(a.fieldvalue,''))) as fieldvalue
            from 
            (
                select 
                adset_id,
                fieldvalue
                from
                ods.ods_s_ams_ams_test_tb_adset_details
                where
                fieldname in ('facebook_positions')
                group by adset_id,fieldvalue           
            ) a 
            group by a.adset_id
        ) o
        on x.adset_id=o.adset_id

        left join 
        (
            select 
            a.adset_id,
            concat_ws(',',collect_set(nvl(a.fieldvalue,''))) as fieldvalue
            from 
            (
                select 
                adset_id,
                fieldvalue
                from
                ods.ods_s_ams_ams_test_tb_adset_details
                where
                fieldname in ('publisher_platforms')
                group by adset_id,fieldvalue           
            ) a 
            group by a.adset_id
        ) p
        on x.adset_id=p.adset_id

        left join 
        (
            select 
            a.id,
            a.adset_id,
            a.fieldname,
            a.fieldvalue,
            a.num
            from 
            (
                select 
                id,
                adset_id,
                fieldname,
                fieldvalue,
                row_number() over ( partition by adset_id order by id desc  ) num 
                from
                ods.ods_s_ams_ams_test_tb_adset_details
                where
                fieldname in ('genders')
                group by adset_id,fieldname,fieldvalue,id
            ) a 
            where a.num=1
        ) q
        on x.adset_id=q.adset_id


        left join 
        (
            select 
            a.adset_id,
            concat_ws(',',collect_set(nvl(a.fielddesc,''))) as user_fb_interests
            from 
            (
                select 
                adset_id,
                fielddesc
                from
                ods.ods_s_ams_ams_test_tb_adset_details
                where
                fieldname in ('interests')
                group by adset_id,fielddesc           
            ) a 
            group by a.adset_id

        ) r
        on x.adset_id=r.adset_id
) b
where
 b.user_fb_age_range !='-' 
 or b.user_fb_banner_click !='' 
 or b.user_fb_platform  !='' 
 or b.user_fb_interests !='' 
 or b.user_fb_genders !='' 
;



 create table if not exists dw_zaful_recommend.zaful_app_abset_id_user_fb_cookieid_fb(
 appsflyer_device_id                        STRING    comment 'cookieid',
 platform                                   STRING    comment '平台',
 language                                   STRING    comment '语言',
 media_source                               STRING    comment '媒体渠道',
 country_code                               STRING    comment '国家简码', 
 fb_adset_id                                STRING    comment '广告id',     
 user_fb_all                                STRING    comment '广告信息'
 )
 PARTITIONED BY (add_time STRING)
 ROW FORMAT DELIMITED FIELDS TERMINATED BY '\u0001' LINES TERMINATED BY '\n' STORED AS TEXTFILE
 ;

insert overwrite table dw_zaful_recommend.zaful_app_abset_id_user_fb_cookieid_fb PARTITION (add_time='${ADD_TIME}')
select
b.appsflyer_device_id,
b.platform,
b.language,
b.media_source,
b.country_code,
case when fb_adset_id !='' then fb_adset_id else null end,
x.user_fb_all
from
(
    select 
    fb_adset_id,
    appsflyer_device_id,
    platform,
    language,
    media_source,
    country_code
    from 
    (
        select 
        fb_adset_id,
        appsflyer_device_id,
        platform,
        language,
        media_source,
        country_code,
        row_number() over ( partition by appsflyer_device_id order by fb_adset_id desc  ) num 
        from
        ods.ods_app_burial_log
        where
        concat(year,month,day) = '${ADD_TIME}'
        and site='zaful'
        --and fb_adset_id is not null and fb_adset_id !=''
        group by 
        fb_adset_id,
        appsflyer_device_id,
        platform,
        language,
        media_source,
        country_code
    ) a 
    where a.num=1    
) b

left join 
dw_zaful_recommend.zaful_app_abset_id_user_fb_all x
on b.fb_adset_id=x.adset_id
;
