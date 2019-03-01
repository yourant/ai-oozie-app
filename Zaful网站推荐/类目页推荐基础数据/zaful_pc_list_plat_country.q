--@author zhanrui
--@date 2019年01月31日 
--@desc  zaful类目页算法所需基础数据任务

SET mapred.job.name='zaful_pc_list_plat_country';
set mapred.job.queue.name=root.ai.offline; 
SET mapred.max.split.size=128000000;
SET mapred.min.split.size=64000000;
SET mapred.min.split.size.per.node=64000000;
SET mapred.min.split.size.per.rack=64000000;
SET hive.exec.reducers.bytes.per.reducer = 64000000;
SET hive.merge.mapfiles=true;
SET hive.merge.mapredfiles= true;
SET hive.input.format=org.apache.hadoop.hive.ql.io.CombineHiveInputFormat;
SET hive.merge.size.per.task=256000000;
SET hive.exec.parallel = true;
set hive.support.concurrency=false;

--最终结果数据汇总
CREATE TABLE IF NOT EXISTS dw_zaful_recommend.zaful_pc_list_plat_country(
    plat             string   COMMENT '平台', 
    country          string   COMMENT '国家', 
    cat_id           string   COMMENT '分类', 
  	goods_sn         string   COMMENT '商品SKU',
  	pv_count         bigint   COMMENT '商品曝光数量',
    ipv_count        bigint   COMMENT '商品点击数量',
    bag_count        bigint   COMMENT '商品加购数量',
    favorite_count   bigint   COMMENT '商品收藏数量',
    order_num        bigint   COMMENT '商品下单数量',
    purchase_num     bigint   COMMENT '商品购买数量',
    gmv              decimal(10,2)    COMMENT 'GMV',
    sales            decimal(10,2)    COMMENT '付款金额',
    price            string   COMMENT '当时的商品价格',
    discount_mark    string   COMMENT '当时的折扣标',  
   	timestamp        bigint   COMMENT '时间戳'
	)
COMMENT "类目页SKU数据统计"
PARTITIONED BY (pdate string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\u0001' 
LINES TERMINATED BY '\n' 
STORED AS TEXTFILE;



---曝光
CREATE TABLE IF NOT EXISTS dw_zaful_recommend.zaful_pc_list_pv_tmp(
    plat        string   COMMENT '平台', 
    country     string   COMMENT '国家', 
    cat_id      string   COMMENT '分类', 
	goods_sn    string   COMMENT '商品SKU',
	pv_count    bigint   COMMENT '商品曝光数量',
    price       string   COMMENT '当时的商品价格',
    discount_mark    string   COMMENT '当时的折扣标',  
   	date        string   COMMENT '日期'
	)
COMMENT "类目页SKU曝光数据统计"
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\u0001' 
LINES TERMINATED BY '\n' 
STORED AS TEXTFILE;



--曝光
INSERT OVERWRITE TABLE dw_zaful_recommend.zaful_pc_list_pv_tmp
SELECT
	platform,
	country_code,
	cat_id,
    goods_sn,
    count(goods_sn) as pv_count,
    'price',
    'discount_mark',
    date
FROM(
	SELECT
		a.date,
		get_json_object(a.sub_event_field,'$.sku') as goods_sn,
        get_json_object(a.sub_event_field,'$.price') as price,
        get_json_object(a.sub_event_field,'$.discount_mark') as discount_mark,
        b.platform,
        b.country_code,
        b.cat_id
	FROM(
		SELECT
			log_id,
			concat(year,month,day) date,
			sub_event_field
		FROM
			ods.ods_pc_burial_log_ubcta
		WHERE
			concat(year,month,day) = '${ADD_TIME}'
            and site='zaful'
		) a
	JOIN(
		SELECT
			log_id,
            platform,
            country_code,
            regexp_extract(page_code, 'category-(.*)',1) as cat_id
		FROM
			ods.ods_pc_burial_log
		WHERE
		    concat(year,month,day) = '${ADD_TIME}'
            and site='zaful'
            AND behaviour_type = 'ie' AND  sub_event_field != '' AND platform in ('pc','m')
            and page_module = 'mp'
			and page_main_type='b'
            and page_code rlike 'category'
		) b
	ON
		a.log_id = b.log_id
	) tmp
WHERE
	goods_sn != '' AND goods_sn IS NOT NULL
GROUP BY
	platform,
	country_code,
	cat_id,
    goods_sn,
    -- price,
    -- discount_mark,
    date;


---点击
CREATE TABLE IF NOT EXISTS dw_zaful_recommend.zaful_pc_list_ipv_tmp(
    plat        string   COMMENT '平台', 
    country     string   COMMENT '国家', 
    cat_id      string   COMMENT '分类', 
	goods_sn    string   COMMENT '商品SKU',
	ipv_count    bigint   COMMENT '商品点击数量',
   	date        string   COMMENT '日期'
	)
COMMENT "类目页SKU点击数据统计"
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\u0001' 
LINES TERMINATED BY '\n' 
STORED AS TEXTFILE;


--点击
INSERT OVERWRITE TABLE dw_zaful_recommend.zaful_pc_list_ipv_tmp
SELECT
	platform,
	country_code,
    cat_id,
    goods_sn,
	COUNT(*) as ipv_count,
    date
FROM(
	SELECT
		concat(year,month,day) date,
		get_json_object(a.skuinfo,'$.sku') as goods_sn,
        platform,
        country_code,
        regexp_extract(page_code, 'category-(.*)',1) as cat_id
	FROM
		ods.ods_pc_burial_log a
	WHERE 
		concat(year,month,day) = '${ADD_TIME}'
        and site='zaful'
        AND behaviour_type = 'ic' AND  sub_event_field != '' AND platform in ('pc','m')
        and page_module = 'mp' and sub_event_info in ('addtobag','sku')
		and page_main_type='b'
        and page_code rlike 'category'
	) tmp
GROUP BY
	platform,
	country_code,
    cat_id,
    goods_sn,
    date;

---加购
CREATE TABLE IF NOT EXISTS dw_zaful_recommend.zaful_pc_list_bag_tmp(
    plat        string   COMMENT '平台', 
    country     string   COMMENT '国家', 
    cat_id      string   COMMENT '分类', 
	goods_sn    string   COMMENT '商品SKU',
	bag_count    bigint   COMMENT '商品加购数量',
   	date        string   COMMENT '日期'
	)
COMMENT "类目页SKU加购数据统计"
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\u0001' 
LINES TERMINATED BY '\n' 
STORED AS TEXTFILE;


--加购
INSERT OVERWRITE TABLE dw_zaful_recommend.zaful_pc_list_bag_tmp
SELECT
	platform,
	country_code,
    cat_id,
    goods_sn,
	sum(bag_count) as bag_count,
    date
FROM(
	SELECT
		concat(year,month,day) date,
		get_json_object(a.skuinfo,'$.sku') as goods_sn,
        get_json_object(a.skuinfo,'$.pam') as bag_count,
        platform,
        country_code,
        regexp_extract(get_json_object(a.sub_event_field,'$.p'), 'category-(.*)',1) as cat_id
	FROM
		ods.ods_pc_burial_log a
	WHERE 
		concat(year,month,day) = '${ADD_TIME}'
        and site='zaful'
        AND behaviour_type = 'ic' AND  sub_event_field != '' AND platform in ('pc','m')
        and sub_event_info ='ADT'
        and get_json_object(a.sub_event_field,'$.fmd')='mp'
        and get_json_object(a.sub_event_field,'$.p') rlike 'category'
	) tmp
GROUP BY
	platform,
	country_code,
    cat_id,
    goods_sn,
    date;


---收藏
CREATE TABLE IF NOT EXISTS dw_zaful_recommend.zaful_pc_list_favorite_tmp(
    plat        string   COMMENT '平台', 
    country     string   COMMENT '国家', 
    cat_id      string   COMMENT '分类', 
	goods_sn    string   COMMENT '商品SKU',
	favorite_count    bigint   COMMENT '商品收藏数量',
   	date        string   COMMENT '日期'
	)
COMMENT "类目页SKU收藏数据统计"
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\u0001' 
LINES TERMINATED BY '\n' 
STORED AS TEXTFILE;


--收藏
INSERT OVERWRITE TABLE dw_zaful_recommend.zaful_pc_list_favorite_tmp
SELECT
	platform,
	country_code,
    cat_id,
    goods_sn,
	count(goods_sn) as favorite_count,
    date
FROM(
	SELECT
		concat(year,month,day) date,
		get_json_object(a.skuinfo,'$.sku') as goods_sn,
        platform,
        country_code,
        regexp_extract(get_json_object(a.sub_event_field,'$.p'), 'category-(.*)',1) as cat_id
	FROM
		ods.ods_pc_burial_log a
	WHERE 
		concat(year,month,day) = '${ADD_TIME}'
        and site='zaful'
        AND behaviour_type = 'ic' AND  sub_event_field != '' AND platform in ('pc','m')
        and sub_event_info ='ADF'
        and get_json_object(a.sub_event_field,'$.fmd')='mp'
        and get_json_object(a.sub_event_field,'$.p') rlike 'category'
	) tmp
GROUP BY
	platform,
	country_code,
    cat_id,
    goods_sn,
    date;


--中间表report_sku_user_tmp：取sku,user_id
CREATE TABLE IF NOT EXISTS dw_zaful_recommend.zaful_pc_list_sku_user_tmp1(
goods_sn                  STRING         COMMENT "sku",
user_id                   STRING         COMMENT "用户ID",
plat                      STRING         COMMENT "平台",
country                   STRING         COMMENT "国家",
cat_id                    STRING         COMMENT "分类ID",
date                      string         COMMENT '日期'
)
COMMENT "类目页user-sku中间表1"
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\u0001' LINES TERMINATED BY '\n' STORED AS TEXTFILE;

CREATE TABLE IF NOT EXISTS dw_zaful_recommend.zaful_pc_list_sku_user_tmp2(
goods_sn                  STRING         COMMENT "sku",
user_id                   STRING         COMMENT "用户ID",
order_status              STRING         COMMENT "订单状态",
pay_amount                decimal(10,2)  COMMENT "购买金额",
goods_number              STRING         COMMENT "商品数量"
)
COMMENT "类目页user-sku中间表2"
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\u0001' LINES TERMINATED BY '\n' STORED AS TEXTFILE;

--ODS数据更新cookie-user 对应关系
INSERT OVERWRITE TABLE dw_zaful_recommend.zaful_od_u_map
SELECT
  m.glb_od,
  m.glb_u
from
  (
    SELECT
      glb_od,
      glb_u
    FROM
      dw_zaful_recommend.zaful_od_u_map
    union all
    SELECT
      cookie_id as glb_od,
      user_id as glb_u
    FROM
      ods.ods_pc_burial_log
    WHERE
      concat_ws(year, month, day) = '${ADD_TIME}'
      and site='zaful'
      AND user_id rlike '^[0-9]+$'
      union all
      select
      cookie_id as glb_od,
      user_id as glb_u
      from 
      ods.ods_php_burial_log
      where
      concat_ws(year, month, day) = '${ADD_TIME}'
      and site='zaful'
      AND user_id rlike '^[0-9]+$'
  ) m
GROUP BY
  m.glb_od,
  m.glb_u
;

--中间表1：取sku,user_id
INSERT OVERWRITE TABLE dw_zaful_recommend.zaful_pc_list_sku_user_tmp1
SELECT
  m.goods_sn,
  n.glb_u,
  m.platform,
  m.country_code,
  m.cat_id,
  m.date
FROM
  (
    SELECT
		concat(year,month,day) date,
		get_json_object(a.skuinfo,'$.sku') as goods_sn,
        cookie_id,
        platform,
        country_code,
        regexp_extract(get_json_object(a.sub_event_field,'$.p'), 'category-(.*)',1) as cat_id
	FROM
		ods.ods_pc_burial_log a
	WHERE 
		concat(year,month,day) BETWEEN '${ADD_TIME_W}'
        AND '${ADD_TIME}'
        and site='zaful'
        AND behaviour_type = 'ic' AND  sub_event_field != '' AND platform in ('pc','m')
        and sub_event_info ='ADT'
        and get_json_object(a.sub_event_field,'$.fmd')='mp'
        and get_json_object(a.sub_event_field,'$.p') rlike 'category'
  ) m
  INNER JOIN dw_zaful_recommend.zaful_od_u_map n ON m.cookie_id = n.glb_od
GROUP BY
  m.goods_sn,
  n.glb_u,
  m.platform,
  m.country_code,
  m.cat_id,
  m.date;


--中间表2：取sku,user_id
INSERT OVERWRITE TABLE dw_zaful_recommend.zaful_pc_list_sku_user_tmp2
SELECT
  p.goods_sn,
  x.user_id,
  x.order_status,
  p.pay_amount,
  p.goods_number
FROM
  (
    SELECT
      order_id,
      user_id,
      add_time,
      order_status
    FROM
      ods.ods_m_zaful_eload_order_info
    WHERE
      dt = '${ADD_TIME}' 
      and from_unixtime(add_time + 8*3600, 'yyyyMMdd') = '${ADD_TIME}'
  ) x
  JOIN   (
  SELECT goods_sn,order_id,
  goods_number,
  case when goods_pay_amount <> '0' then goods_pay_amount
  else goods_price*goods_number end as pay_amount
  from
   ods.ods_m_zaful_eload_order_goods 
   WHERE
      dt = '${ADD_TIME}' 
   ) p
  ON x.order_id = p.order_id
group by
  p.goods_sn,
  x.user_id,
  x.order_status,
  p.pay_amount,
  p.goods_number
;


---下单商品数
CREATE TABLE IF NOT EXISTS dw_zaful_recommend.zaful_pc_list_order_tmp(
    plat        string   COMMENT '平台', 
    country     string   COMMENT '国家', 
    cat_id      string   COMMENT '分类', 
	goods_sn    string   COMMENT '商品SKU',
	order_num    int     COMMENT '下单商品数',
   	date        string   COMMENT '日期'
	)
COMMENT "类目页SKU 下单商品数统计"
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\u0001' 
LINES TERMINATED BY '\n' 
STORED AS TEXTFILE;

--下单商品数
INSERT OVERWRITE TABLE dw_zaful_recommend.zaful_pc_list_order_tmp
SELECT
  x1.plat,
  x1.country,
  x1.cat_id,
  x1.goods_sn,
  SUM(x2.goods_number) AS order_num,
  '${ADD_TIME}' as date
from
  dw_zaful_recommend.zaful_pc_list_sku_user_tmp1 x1
  INNER JOIN dw_zaful_recommend.zaful_pc_list_sku_user_tmp2 x2 ON x1.user_id = x2.user_id
  AND x1.goods_sn = x2.goods_sn
group by
  x1.plat,
  x1.country,
  x1.cat_id,
  x1.goods_sn
;

---付款商品数
CREATE TABLE IF NOT EXISTS dw_zaful_recommend.zaful_pc_list_purchase_tmp(
    plat        string   COMMENT '平台', 
    country     string   COMMENT '国家', 
    cat_id      string   COMMENT '分类', 
	goods_sn    string   COMMENT '商品SKU',
	purchase_num    int     COMMENT '付款商品数',
   	date        string   COMMENT '日期'
	)
COMMENT "类目页SKU 付款商品数统计"
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\u0001' 
LINES TERMINATED BY '\n' 
STORED AS TEXTFILE;


--付款商品数
INSERT OVERWRITE TABLE dw_zaful_recommend.zaful_pc_list_purchase_tmp
SELECT
  x1.plat,
  x1.country,
  x1.cat_id,
  x1.goods_sn,
  SUM(x2.goods_number) AS purchase_num,
  '${ADD_TIME}' as date
from
  dw_zaful_recommend.zaful_pc_list_sku_user_tmp1 x1
  INNER JOIN dw_zaful_recommend.zaful_pc_list_sku_user_tmp2 x2 ON x1.user_id = x2.user_id
  AND x1.goods_sn = x2.goods_sn
  where
  x2.order_status not in ('0','10','11','13')
group by
  x1.plat,
  x1.country,
  x1.cat_id,
  x1.goods_sn
;

---GMV(下单金额)
CREATE TABLE IF NOT EXISTS dw_zaful_recommend.zaful_pc_list_gmv_tmp(
    plat        string   COMMENT '平台', 
    country     string   COMMENT '国家', 
    cat_id      string   COMMENT '分类', 
	goods_sn    string   COMMENT '商品SKU',
	gmv         decimal(10,2)   COMMENT 'GMV',
   	date        string   COMMENT '日期'
	)
COMMENT "类目页SKU GMV数据统计"
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\u0001' 
LINES TERMINATED BY '\n' 
STORED AS TEXTFILE;

--GMV
INSERT OVERWRITE TABLE dw_zaful_recommend.zaful_pc_list_gmv_tmp
SELECT
  x1.plat,
  x1.country,
  x1.cat_id,
  x1.goods_sn,
  SUM(x2.pay_amount) AS gmv,
  '${ADD_TIME}' as date
from
  dw_zaful_recommend.zaful_pc_list_sku_user_tmp1 x1
  INNER JOIN dw_zaful_recommend.zaful_pc_list_sku_user_tmp2 x2 ON x1.user_id = x2.user_id
  AND x1.goods_sn = x2.goods_sn
group by
  x1.plat,
  x1.country,
  x1.cat_id,
  x1.goods_sn
;

---sales(付款金额)
CREATE TABLE IF NOT EXISTS dw_zaful_recommend.zaful_pc_list_sales_tmp(
    plat        string   COMMENT '平台', 
    country     string   COMMENT '国家', 
    cat_id      string   COMMENT '分类', 
	goods_sn    string   COMMENT '商品SKU',
	sales       decimal(10,2)   COMMENT '付款金额',
   	date        string   COMMENT '日期'
	)
COMMENT "类目页SKU 付款金额数据统计"
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\u0001' 
LINES TERMINATED BY '\n' 
STORED AS TEXTFILE;

--sales
INSERT OVERWRITE TABLE dw_zaful_recommend.zaful_pc_list_sales_tmp
SELECT
  x1.plat,
  x1.country,
  x1.cat_id,
  x1.goods_sn,
  SUM(x2.pay_amount) AS sales,
  '${ADD_TIME}' as date
from
  dw_zaful_recommend.zaful_pc_list_sku_user_tmp1 x1
  INNER JOIN dw_zaful_recommend.zaful_pc_list_sku_user_tmp2 x2 ON x1.user_id = x2.user_id
  AND x1.goods_sn = x2.goods_sn
where
  x2.order_status not in ('0','10','11','13')
group by
  x1.plat,
  x1.country,
  x1.cat_id,
  x1.goods_sn
;


--最终结果汇总
INSERT OVERWRITE TABLE dw_zaful_recommend.zaful_pc_list_plat_country partition(pdate = '${ADD_TIME}')
SELECT
NVL(t1.plat,''),
NVL(t1.country,''),
NVL(t1.cat_id,''),
NVL(t1.goods_sn,''),
NVL(t1.pv_count,0),
NVL(t2.ipv_count,0),
NVL(t3.bag_count,0),
NVL(t4.favorite_count,0),
NVL(t5.order_num,0),
NVL(t6.purchase_num,0),
NVL(t7.gmv,0),
NVL(t8.sales,0),
NVL(t1.price,''),
NVL(t1.discount_mark,''),
NVL(to_unix_timestamp(t1.date,"yyyyMMdd"),0)
FROM dw_zaful_recommend.zaful_pc_list_pv_tmp t1
LEFT JOIN dw_zaful_recommend.zaful_pc_list_ipv_tmp t2 
ON t1.plat = t2.plat AND t1.country = t2.country AND t1.cat_id = t2.cat_id AND t1.goods_sn = t2.goods_sn
LEFT JOIN dw_zaful_recommend.zaful_pc_list_bag_tmp t3
ON t1.plat = t3.plat AND t1.country = t3.country AND t1.cat_id = t3.cat_id AND t1.goods_sn = t3.goods_sn
LEFT JOIN dw_zaful_recommend.zaful_pc_list_favorite_tmp t4
ON t1.plat = t4.plat AND t1.country = t4.country AND t1.cat_id = t4.cat_id AND t1.goods_sn = t4.goods_sn
LEFT JOIN dw_zaful_recommend.zaful_pc_list_order_tmp t5
ON t1.plat = t5.plat AND t1.country = t5.country AND t1.cat_id = t5.cat_id AND t1.goods_sn = t5.goods_sn
LEFT JOIN dw_zaful_recommend.zaful_pc_list_purchase_tmp t6
ON t1.plat = t6.plat AND t1.country = t6.country AND t1.cat_id = t6.cat_id AND t1.goods_sn = t6.goods_sn
LEFT JOIN dw_zaful_recommend.zaful_pc_list_gmv_tmp t7
ON t1.plat = t7.plat AND t1.country = t7.country AND t1.cat_id = t7.cat_id AND t1.goods_sn = t7.goods_sn
LEFT JOIN dw_zaful_recommend.zaful_pc_list_sales_tmp t8
ON t1.plat = t8.plat AND t1.country = t8.country AND t1.cat_id = t8.cat_id AND t1.goods_sn = t8.goods_sn
;


--将国家维度进行汇总
INSERT OVERWRITE TABLE dw_zaful_recommend.zaful_pc_list_plat_global partition(pdate = '${ADD_TIME}')
SELECT 
    plat                  , 
    cat_id                , 
  	goods_sn              ,
  	sum(pv_count)         ,
    sum(ipv_count)        ,
    sum(bag_count)        ,
    sum(favorite_count)   ,
    sum(order_num)        ,
    sum(purchase_num)     ,
    sum(gmv)              ,
    sum(sales)            ,
    price                 ,
    discount_mark         ,  
   	timestamp 
FROM   dw_zaful_recommend.zaful_pc_list_plat_country
WHERE pdate = '${ADD_TIME}'   
GROUP BY 
    plat           , 
    cat_id         , 
  	goods_sn       ,
    price          ,
    discount_mark  ,  
   	timestamp
; 