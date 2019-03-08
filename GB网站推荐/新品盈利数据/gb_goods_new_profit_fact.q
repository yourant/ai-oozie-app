--@author fangrz
--@date 2018年11月21日 
--@desc  gb商品自动打标 

SET mapred.job.name=gb_goods_new_profit_fact;
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
set hive.auto.convert.join=true;



CREATE TABLE IF NOT EXISTS dw_gearbest_recommend.gb_goods_new_profit_fact  
 (   
	sku  string  COMMENT '商品id',     
	is_new string COMMENT '新品1是0否',
	
	is_profit string COMMENT '毛利润大于百分之三十1是0否') 

COMMENT 'gb商品自动打标表' 
PARTITIONED BY (dt string) 
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' 
LINES TERMINATED BY '\n' 
STORED AS TEXTFILE;

insert overwrite table dw_gearbest_recommend.gb_goods_new_profit_fact  partition (dt=${YEAR}${MONTH}${DAY}) 
select 	
	t01.good_sn, 	
	nvl(t01.new_good,0),	
	nvl(t11.is_profit,0) 
from 	
	(select 		
		t0.good_sn,		
		'1' as new_good   	
	from 		
		(select 
			good_sn 
		from 
			stg.gb_goods_goods_info 
		group by 
			good_sn
		) t0 	
	join 		
		(select 
			good_sn 
		from 
			stg.gb_goods_goods_info_extend 
		where 
			first_up_time is not NULL 
			and  from_unixtime(first_up_time,'yyyyMMdd') between from_unixtime(unix_timestamp(date_sub('${YEAR}-${MONTH}-${DAY}',60),'yyyy-mm-dd'),'yyyymmdd') and ${YEAR}${MONTH}${DAY}  
		group by 
			good_sn
		) t1  	
	on 			
		t0.good_sn=t1.good_sn 			
	group by 		
		t0.good_sn 			
	)t01  
full join 	
	(select 		
		t2.good_sn,		
		'1' as is_profit 	
	from 		
		(select 
			/*+ mapjoin(t0)*/ 			
			t1.good_sn,			
			if(t0.price - t1.ship_price/t1.exchange_rate<=0,-1,(t0.price - (t1.ship_price +t1.ship_fee)/t1.exchange_rate)/abs(t0.price - t1.ship_price/t1.exchange_rate)) as margin_profit  		
		from 		
			ods.ods_m_gearbest_gb_order_order_goods t0 		
		join 			
			ods.ods_m_gearbest_goods_price_factor t1 		
		on 			
			t0.goods_sn = t1.good_sn 
			and t0.price_md5=t1.price_md5 
			and t0.warehouse_code = t1.wh_code 		
			and t0.dt=${YEAR}${MONTH}${DAY}   			
			and t1.dt=${YEAR}${MONTH}${DAY}  
		) t2 	
	where 		
		t2.margin_profit*100>30 	
	)t11  
on 	
	t01.good_sn=t11.good_sn 

	and t01.good_sn is not NULL 
	and t11.good_sn is not NULL 
group by 
	
	t01.good_sn, 	
	t01.new_good,
	t11.is_profit;


--语句放在新增，盈利表之后运行，将信息补充到表data_input_lable_input，单独设置分区
INSERT OVERWRITE TABLE dw_gearbest_recommend.data_input_lable_input PARTITION(pdate = 'wuchao',lable='3')
SELECT
    ROW_NUMBER() OVER(PARTITION BY ss ORDER BY rand()) AS flag,
    good_sn,
    3 label_id,
    '' name
from(
SELECT
    distinct 
	a.sku as good_sn,
	1 ss,
	'',
	''
FROM
	dw_gearbest_recommend.gb_goods_new_profit_fact  a
	
where 
	dt='${YEAR}${MONTH}${DAY}' and sku is not null and is_new=1 
)a

union all

SELECT
    ROW_NUMBER() OVER(PARTITION BY ss ORDER BY rand()) AS flag,
    good_sn,
    4 label_id,
    '' name
from (
SELECT
    distinct 
	a.sku as good_sn,
	1 ss,
	'',
	''
FROM
	dw_gearbest_recommend.gb_goods_new_profit_fact  a	
where 
	dt='${YEAR}${MONTH}${DAY}' and sku is not null and is_new=1 
)a;

