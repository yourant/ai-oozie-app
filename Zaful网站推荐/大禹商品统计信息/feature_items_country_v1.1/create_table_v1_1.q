
--商品基本信息表
CREATE TABLE IF NOT EXISTS dw_zaful_recommend.feature_items_base_info(
    item_id String COMMENT '商品sku编号',
    item_cat_id int COMMENT '商品叶子类目id',
    item_cat_name String COMMENT '商品叶子类目名称',
    item_category_list String COMMENT '商品的类目列表',    
    item_cat1_id String COMMENT '商品的一级类目',
    item_cat2_id String COMMENT '商品的二级类目',
    item_cat3_id String COMMENT '商品的三级类目',
    item_cat4_id String COMMENT '商品的四级类目',
    item_price decimal(10,2) COMMENT '商品的价格',
    item_add_time int COMMENT '商品添加时间',
    item_number int COMMENT '库存',
    item_is_24hour_ship tinyint COMMENT '是否24小时发货',
    item_star_level double COMMENT '商品星级评分',
    item_review_num bigint COMMENT '商品评论数'
    )
PARTITIONED BY (year String, month String, day String)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' LINES TERMINATED BY '\n' 
STORED AS TEXTFILE;

--商品pc平台统计信息日表
CREATE TABLE IF NOT EXISTS dw_zaful_recommend.feature_items_pc (
    item_id String COMMENT '商品sku编号',
    item_pv_cnt bigint COMMENT '商品的pc端曝光数',
    item_pv_uv bigint COMMENT '商品的pc端曝光uv',
    item_pv_per_cnt double COMMENT '商品的pc端人均曝光数',    
    item_ipv_cnt bigint COMMENT '商品的pc端点击数',
    item_ipv_uv bigint COMMENT '商品的pc端点击uv',
    item_ipv_per_cnt double COMMENT '商品的pc端人均点击数',
    item_bag_cnt bigint COMMENT '商品的pc端加购数',
    item_bag_uv bigint COMMENT '商品的pc端加购uv',
    item_bag_per_cnt double COMMENT '商品的pc端人均加购数',
    item_favorite_cnt bigint COMMENT '商品的pc端收藏数',
    item_favorite_uv bigint COMMENT '商品的pc端收藏uv',
    item_favorite_per_cnt double COMMENT '商品的pc端人均收藏数',
    item_order_cnt bigint COMMENT '商品的pc端销量(下单商品数)',
    item_order_uv bigint COMMENT '商品的pc端下单uv',
    item_order_per_cnt double COMMENT '商品的pc端人均下单商品数',
    item_cvr double COMMENT '商品的pc端购买转化率',
    item_uv_cvr double COMMENT '商品的pc端购买uv转化率'
    )
PARTITIONED BY (year String, month String, day String)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' LINES TERMINATED BY '\n' 
STORED AS TEXTFILE;

--商品m平台统计信息日表
CREATE TABLE IF NOT EXISTS dw_zaful_recommend.feature_items_m (
    item_id String COMMENT '商品sku编号',
    item_pv_cnt bigint COMMENT '商品的m端曝光数',
    item_pv_uv bigint COMMENT '商品的m端曝光uv',
    item_pv_per_cnt double COMMENT '商品的m端人均曝光数',    
    item_ipv_cnt bigint COMMENT '商品的m端点击数',
    item_ipv_uv bigint COMMENT '商品的m端点击uv',
    item_ipv_per_cnt double COMMENT '商品的m端人均点击数',
    item_bag_cnt bigint COMMENT '商品的m端加购数',
    item_bag_uv bigint COMMENT '商品的m端加购uv',
    item_bag_per_cnt double COMMENT '商品的m端人均加购数',
    item_favorite_cnt bigint COMMENT '商品的m端收藏数',
    item_favorite_uv bigint COMMENT '商品的m端收藏uv',
    item_favorite_per_cnt double COMMENT '商品的m端人均收藏数',
    item_order_cnt bigint COMMENT '商品的m端销量(下单商品数)',
    item_order_uv bigint COMMENT '商品的m端下单uv',
    item_order_per_cnt double COMMENT '商品的pc端人均下单商品数',
    item_cvr double COMMENT '商品的m端购买转化率',
    item_uv_cvr double COMMENT '商品的m端购买uv转化率'
    )
PARTITIONED BY (year String, month String, day String)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' LINES TERMINATED BY '\n' 
STORED AS TEXTFILE;

--商品ios平台统计信息日表
CREATE TABLE IF NOT EXISTS dw_zaful_recommend.feature_items_ios (
    item_id String COMMENT '商品sku编号',
    item_ios_pv_cnt bigint COMMENT '商品的m端曝光数',
    item_ios_pv_uv bigint COMMENT '商品的m端曝光uv',
    item_ios_pv_per_cnt double COMMENT '商品的m端人均曝光数',    
    item_ios_ipv_cnt bigint COMMENT '商品的m端点击数',
    item_ios_ipv_uv bigint COMMENT '商品的m端点击uv',
    item_ios_ipv_per_cnt double COMMENT '商品的m端人均点击数',
    item_ios_bag_cnt bigint COMMENT '商品的m端加购数',
    item_ios_bag_uv bigint COMMENT '商品的m端加购uv',
    item_ios_bag_per_cnt double COMMENT '商品的m端人均加购数',
    item_ios_favorite_cnt bigint COMMENT '商品的m端收藏数',
    item_ios_favorite_uv bigint COMMENT '商品的m端收藏uv',
    item_ios_favorite_per_cnt double COMMENT '商品的m端人均收藏数',
    item_ios_order_cnt bigint COMMENT '商品的m端销量(下单商品数)',
    item_ios_order_uv bigint COMMENT '商品的m端下单uv',
    item_ios_order_per_cnt double COMMENT '商品的pc端人均下单商品数',
    item_ios_cvr double COMMENT '商品的m端购买转化率',
    item_ios_uv_cvr double COMMENT '商品的m端购买uv转化率'
    )
PARTITIONED BY (year String, month String, day String)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' LINES TERMINATED BY '\n' 
STORED AS TEXTFILE;

--商品android平台统计信息日表
CREATE TABLE IF NOT EXISTS dw_zaful_recommend.feature_items_android (
    item_id String COMMENT '商品sku编号',
    item_android_pv_cnt bigint COMMENT '商品的m端曝光数',
    item_android_pv_uv bigint COMMENT '商品的m端曝光uv',
    item_android_pv_per_cnt double COMMENT '商品的m端人均曝光数',    
    item_android_ipv_cnt bigint COMMENT '商品的m端点击数',
    item_android_ipv_uv bigint COMMENT '商品的m端点击uv',
    item_android_ipv_per_cnt double COMMENT '商品的m端人均点击数',
    item_android_bag_cnt bigint COMMENT '商品的m端加购数',
    item_android_bag_uv bigint COMMENT '商品的m端加购uv',
    item_android_bag_per_cnt double COMMENT '商品的m端人均加购数',
    item_android_favorite_cnt bigint COMMENT '商品的m端收藏数',
    item_android_favorite_uv bigint COMMENT '商品的m端收藏uv',
    item_android_favorite_per_cnt double COMMENT '商品的m端人均收藏数',
    item_android_order_cnt bigint COMMENT '商品的m端销量(下单商品数)',
    item_android_order_uv bigint COMMENT '商品的m端下单uv',
    item_android_order_per_cnt double COMMENT '商品的pc端人均下单商品数',
    item_android_cvr double COMMENT '商品的m端购买转化率',
    item_android_uv_cvr double COMMENT '商品的m端购买uv转化率'
    )
PARTITIONED BY (year String, month String, day String)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' LINES TERMINATED BY '\n' 
STORED AS TEXTFILE;










insert overwrite table dw_zaful_recommend.feature_items partition (platform='pc', year=${YEAR},month=${MONTH},day=${DAY})  
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




--创建表feature_items_v1_1
CREATE TABLE IF NOT EXISTS dw_zaful_recommend.feature_items_v1_1 (
    item_id String COMMENT '商品sku编号',
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
    order_cnt bigint COMMENT '商品的pc端销量(下单商品数)',
    order_uv  bigint COMMENT '商品的pc端下单uv',
    order_per_cnt double COMMENT '商品的pc端人均下单商品数',
    cvr double COMMENT '商品的pc端购买转化率',
    uv_cvr double COMMENT '商品的pc端购买uv转化率'
    )
PARTITIONED BY (platform String, year String, month String, day String)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' LINES TERMINATED BY '\n' 
STORED AS TEXTFILE;


--创建表feature_items_v1_1
CREATE TABLE IF NOT EXISTS dw_zaful_recommend.feature_items_v1_1 (
    item_id String COMMENT '商品sku编号',
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
    order_cnt bigint COMMENT '商品的pc端销量(下单商品数)',
    order_uv  bigint COMMENT '商品的pc端下单uv',
    order_per_cnt double COMMENT '商品的pc端人均下单商品数',
    cvr double COMMENT '商品的pc端购买转化率',
    uv_cvr double COMMENT '商品的pc端购买uv转化率'
    )
PARTITIONED BY (platform String, year String, month String, day String)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' LINES TERMINATED BY '\n' 
STORED AS TEXTFILE;




--创建表feature_items_country_v1_1
CREATE TABLE IF NOT EXISTS dw_zaful_recommend.feature_items_country_v1_1 (
    item_id String COMMENT '商品sku编号',
    country String COMMENT '国家编码',
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
    order_cnt bigint COMMENT '商品的pc端销量(下单商品数)',
    order_uv  bigint COMMENT '商品的pc端下单uv',
    order_per_cnt double COMMENT '商品的pc端人均下单商品数',
    cvr double COMMENT '商品的pc端购买转化率',
    uv_cvr double COMMENT '商品的pc端购买uv转化率'
    )
PARTITIONED BY (platform String, year String, month String, day String)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' LINES TERMINATED BY '\n' 
STORED AS TEXTFILE;
