--@author ZhanRui
--@date 2018年9月28日 
--@desc  ys_goods_on_sale

SET mapred.job.name=ys_goods_on_sale;
SET mapred.max.split.size=128000000;
SET mapred.min.split.size=32000000;
SET mapred.min.split.size.per.node=32000000;
SET mapred.min.split.size.per.rack=32000000;
SET hive.exec.reducers.bytes.per.reducer = 128000000;
SET hive.merge.mapfiles=true;
SET hive.merge.mapredfiles= true;
SET hive.merge.size.per.task=256000000;


INSERT OVERWRITE TABLE dw_zaful_recommend.ys_goods_on_sale  
SELECT
	m.*
FROM
	stg.ys_goods m
JOIN stg.ys_warehouse_goods n ON m.goods_id = n.goods_id
AND n.is_on_sale = '1';

