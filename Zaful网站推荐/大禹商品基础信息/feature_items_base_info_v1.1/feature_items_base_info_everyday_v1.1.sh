#!/bin/sh
today=$(date +%Y%m%d)
day=$(date +%d)

#使用30天浏览数据
start_time=$(date -d "today -1day" +%Y%m%d)
DATE=$(date -d "$start_time" +%Y%m%d)
YEAR=$(date -d "$start_time" +%Y)
MONTH=$(date -d "$start_time" +%m)
DAY=$(date -d "$start_time" +%d)

hive -e"
set mapred.job.queue.name = root.ai.offline;
--商品类目信息
insert overwrite table zaful_recommend.sku_category  
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
              stg.zaful_eload_category
          ) a
      ) b
  ) m
  join stg.zaful_eload_goods n on m.cat_id = n.cat_id
where
  length(n.goods_sn) = 9;


set mapred.job.queue.name = root.ai.offline;
--商品价格、库存、星级、评论信息
insert overwrite TABLE zaful_recommend.zaful_item 
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
    -- sku的价格、库存、是否24小时发货、sku的添加时间
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
          stg.zaful_eload_goods
      ) a
      left join(
        SELECT
          goods_id,
          is_24h_ship
        FROM
          stg.zaful_eload_goods_extend
      ) b on a.goods_id = b.goods_id
  ) n
  LEFT JOIN(
    -- spu的商品评论数、商品星级评分
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
      and a.dt = ${DATE}
      and b.dt = ${DATE}
    GROUP BY
      a.goods_spu,
      a.dt
  ) m ON substr(n.goods_sn, 1, 7) = m.goods_spu
where
  length(n.goods_sn) = 9;


set mapred.job.queue.name = root.ai.offline;
--插入数据
insert overwrite table dw_zaful_recommend.feature_items_base_info partition (year=${YEAR},month=${MONTH},day=${DAY})
select
  b.goods_sn as item_id,
  cat_id as item_cat_id,
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
      zaful_recommend.sku_category
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
      zaful_recommend.zaful_item
  ) c on b.goods_sn = c.item_id;
"