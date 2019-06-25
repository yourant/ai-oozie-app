-- #!/bin/sh
-- today=$(date +%Y%m%d)
-- day=$(date +%d)

-- #使用每天浏览数据
-- start_time=$(date -d "today -1day" +%Y%m%d)
-- DATE=$(date -d "$start_time" +%Y%m%d)
-- YEAR=$(date -d "$start_time" +%Y)
-- MONTH=$(date -d "$start_time" +%m)
-- DAY=$(date -d "$start_time" +%d)
SET mapred.job.name=feature_items_base_info_ods;
set mapred.job.queue.name = root.ai.offline;

--商品类目信息
drop table if exists temp_zaful_recommend.sku_category_ods;
create table temp_zaful_recommend.sku_category_ods as
select
  m.cat_id,
  m.cat_name,
  regexp_replace(m.node, ',', '\073') as nodes,
  m.node1,
  m.node2,
  m.node3,
  m.node4,
  n.goods_sn
from
  (
    select
      b.cat_id,
      b.cat_name,
      b.node,
      split(b.node, ',') [0] as node1,
      split(b.node, ',') [1] as node2,
      split(b.node, ',') [2] as node3,
      split(b.node, ',') [3] as node4
    from
      (
        select
          distinct *
        from
          (
            select
              cat_id,
              cat_name,
              node
            from
              ods.ods_m_zaful_eload_category
            WHERE
              dt= '${DATE}'
          ) a
      ) b
  ) m
  join (
    select
      * 
    from
      ods.ods_m_zaful_eload_goods
    where
      dt = '${DATE}'
  )n on m.cat_id = n.cat_id
where
  length(n.goods_sn) = 9;


--商品价格、库存、星级、评论信息
drop table if exists temp_zaful_recommend.zaful_item_ods;
create table temp_zaful_recommend.zaful_item_ods as
SELECT
  n.goods_sn as item_id,
  n.item_price,
  n.item_is_on_sale,
  n.item_is_delete,
  n.item_number,
  n.item_sale_status,
  n.item_is_24hour_ship,
  m.item_review_num,
  m.item_star_level,
  n.item_add_time,
  m.review_date
FROM(
    -- sku 的 价 格 、 库 存 、 是 否 24 小 时 发 货 、 sku 的 添 加 时 间
    SELECT
      a.goods_sn,
      a.market_price as item_price,
      a.is_on_sale as item_is_on_sale,
      a.is_delete as item_is_delete,
      a.goods_number as item_number,
      a.sale_status as item_sale_status,
      a.add_time as item_add_time,
      b.is_24h_ship as item_is_24hour_ship
    FROM(
        SELECT
          goods_id,
          goods_sn,
          market_price,
          is_on_sale,
          is_delete,
          goods_number,
          case
            when (
              is_on_sale = 1
              and is_delete = 0
              and goods_number > 0
            ) then 1
            else 0
          end as sale_status,
          add_time
        FROM
          ods.ods_m_zaful_eload_goods
        WHERE
          dt = '${DATE}'
      ) a
      left join(
        SELECT
          goods_id,
          is_24h_ship
        FROM
          ods.ods_m_zaful_eload_goods_extend
        WHERE
          dt = '${DATE}'
      ) b on a.goods_id = b.goods_id
  ) n
  LEFT JOIN(
    -- spu 的 商 品 评 论 数 、 商 品 星 级 评 分
    SELECT
      a.goods_spu,
      COUNT(a.id) AS item_review_num,
      round(AVG(b.rate_overall), 1) AS item_star_level,
      a.dt as review_date
    FROM
      ods.ods_m_zaful_review_service_zaful_review a
      LEFT JOIN ods.ods_m_zaful_review_service_zaful_review_info b ON a.id = b.review_id
    WHERE
      a.id > 0
      and status = 1
      and is_del = 0
      and a.lang = 'en'
      and a.dt = '${DATE}'
      and b.dt = '${DATE}'
    GROUP BY
      a.goods_spu,
      a.dt
  ) m ON substr(n.goods_sn, 1, 7) = m.goods_spu
where
  length(n.goods_sn) = 9;



--插入数据
insert overwrite table temp_zaful_recommend.feature_items_base_info_ods partition (year=${YEAR},month=${MONTH},day=${DAY})
select
  b.goods_sn as item_id,
  b.cat_id as item_cat_id,
  b.cat_name as item_cat_name,
  b.nodes as item_category_list,
  (
    case
      when b.node1 is not null then b.node1
      else 'null'
    end
  ) as item_cat1_id,
  (
    case
      when b.node2 is not null then b.node2
      else 'null'
    end
  ) as item_cat2_id,
  (
    case
      when b.node3 is not null then b.node3
      else 'null'
    end
  ) as item_cat3_id,
  (
    case
      when b.node4 is not null then b.node4
      else 'null'
    end
  ) as item_cat4_id,
  c.item_price,
  c.item_add_time,
  c.item_is_on_sale,
  c.item_is_delete,
  c.item_number,
  c.item_sale_status,
  c.item_is_24hour_ship,
  (
    case
      when c.item_star_level is not null then c.item_star_level
      else 0.0
    end
  ) as item_star_level,
  (
    case
      when c.item_review_num is not null then c.item_review_num
      else 0
    end
  ) as item_review_num
from
  (
    SELECT
      goods_sn,
      cat_id,
      nodes,
      cat_name,
      node1,
      node2,
      node3,
      node4
    FROM
      temp_zaful_recommend.sku_category_ods
  ) b
  left join (
    SELECT
      item_id,
      item_price,
      item_is_on_sale,
      item_is_delete,
      item_number,
      item_sale_status,
      item_is_24hour_ship,
      item_review_num,
      item_star_level,
      item_add_time
    FROM
      temp_zaful_recommend.zaful_item_ods
  ) c on b.goods_sn = c.item_id;


CREATE TABLE IF NOT EXISTS temp_zaful_recommend.feature_items_base_info_ods_desc_temp(
    item_id String COMMENT '商品sku编号',
    item_property_list String COMMENT '商品属性列表',
    color String COMMENT '颜色',
    size String COMMENT '尺码',
    Pattern_Type String COMMENT '图案',
    Material String COMMENT '材质',
    date String COMMENT '更新日期',
    style String COMMENT '风格',
    season String COMMENT '季节'
    )
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' LINES TERMINATED BY '\n' 
STORED AS TEXTFILE;

insert overwrite table temp_zaful_recommend.feature_items_base_info_ods_desc_temp
select 
g.goods_sn as item_id,
g.goods_desc desc, 
gc.attr_value color,
gs.attr_value size ,
regexp_extract(regexp_replace(regexp_replace(regexp_replace(g.goods_desc,' ',''),'<strong>PatternType:</strong>','['),'<br/>',']'),'\\[(.+?)\\]',1) as PatternType,
regexp_extract(regexp_replace(regexp_replace(regexp_replace(g.goods_desc,' ',''),'<strong>Material:</strong>','['),'<br/>',']'),'\\[(.+?)\\]',1) as Material,
'${DATE}',
regexp_extract(regexp_replace(regexp_replace(regexp_replace(g.goods_desc,' ',''),'<strong>Style:</strong>','['),'<br/>',']'),'\\[(.+?)\\]',1) as Style,
regexp_extract(regexp_replace(regexp_replace(regexp_replace(g.goods_desc,' ',''),'<strong>Season:</strong>','['),'<br/>',']'),'\\[(.+?)\\]',1) as Season
from
( select * from ods.ods_m_zaful_eload_goods where dt = '${DATE}') g 
LEFT JOIN (select * from ods.ods_m_zaful_eload_goods_attr where dt = '${DATE}' AND attr_id = 7  ) gs ON g.goods_id = gs.goods_id 
LEFT JOIN (select * from ods.ods_m_zaful_eload_goods_attr where dt = '${DATE}' AND attr_id = 8 ) gc ON g.goods_id = gc.goods_id 
where length(g.goods_sn) = 9
group by g.goods_sn,g.goods_desc,gc.attr_value,gs.attr_value  
;


CREATE TABLE IF NOT EXISTS temp_zaful_recommend.feature_items_base_info_ods_desc(
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
    item_is_on_sale int COMMENT '商品是否在售',
    item_is_delete int COMMENT '商品是否已删除',
    item_number int COMMENT '库存',
    item_sale_status int COMMENT '商品在售状态',
    item_is_24hour_ship tinyint COMMENT '是否24小时发货',
    item_star_level double COMMENT '商品星级评分',
    item_review_num bigint COMMENT '商品评论数',
    item_spu_id String COMMENT '商品spu编号',
    item_is_clearance int COMMENT '是否清仓',
    item_favorite_num bigint COMMENT '点赞',
    item_property_list String COMMENT '商品属性列表',
    color String COMMENT '颜色',
    size String COMMENT '尺码',
    Pattern_Type String COMMENT '图案',
    Material String COMMENT '材质',
    date String COMMENT '更新日期',
    style String COMMENT '风格',
    season String COMMENT '季节'
    )
PARTITIONED BY (year String, month String, day String)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' LINES TERMINATED BY '\n' 
STORED AS TEXTFILE;

insert overwrite table temp_zaful_recommend.feature_items_base_info_ods_desc partition (year=${YEAR},month=${MONTH},day=${DAY})
select 
a.item_id,
a.item_cat_id,
a.item_cat_name,
a.item_category_list,
a.item_cat1_id,
a.item_cat2_id,
a.item_cat3_id,
a.item_cat4_id,
a.item_price,
a.item_add_time,
a.item_is_on_sale,
a.item_is_delete,
a.item_number,
a.item_sale_status,
a.item_is_24hour_ship,
a.item_star_level,
a.item_review_num,
b.group_color_goods_id,
b.shelf_down_type,
b.up,
c.item_property_list,
c.color,
c.size,
c.Pattern_Type,
c.Material,
c.date,
c.style,
c.season
from 
    (
    select *
    from 
    temp_zaful_recommend.feature_items_base_info_ods
    where concat(year,month,day) =${DATE}

    ) a
    left join
    (
    select goods_sn,group_color_goods_id,shelf_down_type,up
    from 
    ods.ods_m_zaful_eload_goods
    where dt =${DATE}
    ) b
    on a.item_id=b.goods_sn
    left join
    (
    select *
    from 
    temp_zaful_recommend.feature_items_base_info_ods_desc_temp
    ) c 
    on a.item_id=c.item_id

;



