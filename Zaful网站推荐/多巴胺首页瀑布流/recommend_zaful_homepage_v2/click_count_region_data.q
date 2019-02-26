--@author LiuQingFan
--@date 2018年03月28日 下午 16:00
--@desc  国家、区域点击量数据
set mapred.job.queue.name=root.ai.offline; 
set mapred.job.name='cli_country_num_tmp';
SET mapred.max.split.size=128000000;
SET mapred.min.split.size=128000000;
SET mapred.min.split.size.per.node=128000000;
SET mapred.min.split.size.per.rack=128000000;
SET hive.exec.reducers.bytes.per.reducer = 128000000;
SET hive.merge.mapfiles=true;
SET hive.merge.mapredfiles= true;
SET hive.merge.size.per.task=256000000;
SET hive.exec.parallel = true; 
USE  dw_zaful_recommend;
SET hive.auto.convert.join=false;

set hive.support.concurrency=false;
INSERT OVERWRITE TABLE stg.zaful_ip_region
SELECT
	DISTINCT
	ip,
	city,
	region,
	country
from(
	SELECT
		ip,
		city,
		region,
		country
	FROM
		stg.zaful_ip_region
	UNION ALL
	SELECT
		ip,
		city,
		region,
		country
	FROM
		stg.ip_country_region_table
	WHERE
		concat(year,month,day) =${ADD_TIME}
	)tmp;
	
CREATE TABLE IF NOT EXISTS cli_country_num_tmp(
    date                  string          COMMENT '日期',
	glb_plf               string          COMMENT '平台',
    country               string          comment "国家",
    num                   INT             comment "点击量"                
) comment "国家日点击量超过1000"
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\u0001' LINES TERMINATED BY '\n' STORED AS TEXTFILE;
----点击过1000的国家	
INSERT OVERWRITE  TABLE cli_country_num_tmp
SELECT
	*
FROM
	cli_country_num_tmp
WHERE
	date != ${ADD_TIME};

INSERT INTO cli_country_num_tmp
SELECT
	date,
	glb_plf,
	country,
	COUNT(*) click_num
FROM(
	SELECT
		concat(a.year,a.month,a.day) date,
		a.glb_plf,
		get_json_object(a.glb_skuinfo,'$.sku') goods_sn,
		b.country
	FROM
		stg.zf_pc_event_info a
	JOIN
		stg.zaful_ip_region b
	ON
		a.http_true_client_ip = b.ip
	WHERE 
		a.glb_t = 'ic' AND concat(a.year,a.month,a.day) = ${ADD_TIME}
         AND a.glb_skuinfo != '' AND a.glb_plf in ('pc','m')
         and a.glb_x in ('addtobag','sku')
	) tmp
GROUP BY
	date,
	glb_plf,
	country
HAVING
    click_num > 1000;
	
	
CREATE TABLE IF NOT EXISTS cli_country_region_num_tmp(
    date                  string          COMMENT '日期',
	glb_plf               string          COMMENT '平台',
    country               string          comment "国家",
	region                string          COMMENT '区域',
    num                   INT             comment "点击量"                
) comment "国家日点击量超过1000"
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\u0001' LINES TERMINATED BY '\n' STORED AS TEXTFILE;
----点击过5000的国家,点击过1000的区域
INSERT OVERWRITE  TABLE cli_country_region_num_tmp
SELECT
	*
FROM
	cli_country_region_num_tmp
WHERE
	date != ${ADD_TIME};

	
INSERT INTO   cli_country_region_num_tmp
SELECT
	date,
	glb_plf,
	country,
	region,
	COUNT(*) click_num
FROM(
	SELECT
		concat(a.year,a.month,a.day) date,
		a.glb_plf,
		get_json_object(a.glb_skuinfo,'$.sku') goods_sn,
		b.country,
		b.region
	FROM
		stg.zf_pc_event_info a
	JOIN
		stg.zaful_ip_region b
	ON
		a.http_true_client_ip = b.ip
	WHERE 
		a.glb_t = 'ic' AND concat(a.year,a.month,a.day) = ${ADD_TIME}
         AND a.glb_skuinfo != '' AND a.glb_plf in ('pc','m')
         and a.glb_x in ('addtobag','sku')
	) tmp
GROUP BY
	date,
	glb_plf,
	region,
	country
HAVING
    click_num > 1000;
	
INSERT overwrite table cli_country_region_num_tmp
SELECT
	a.date,
	a.glb_plf,
	a.country,
	a.region,
	a.num
FROM
	cli_country_region_num_tmp a
JOIN(
	SELECT
		date,
		glb_plf,
		country
	FROM
		cli_country_num_tmp 
	WHERE
		num > 5000
	) b
ON
	a.date = b.date AND a.glb_plf = b.glb_plf AND a.country = b.country;
	
	

