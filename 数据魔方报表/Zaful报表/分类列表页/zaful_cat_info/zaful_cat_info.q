
--@author ZhanRui
--@date 2018年6月25日 
--@desc  Zaful分类列表分类信息统计

SET mapred.job.name=zaful_cat_info;
set mapred.job.queue.name=root.ai.offline;
SET mapred.max.split.size=128000000;
SET mapred.min.split.size=128000000;
SET mapred.min.split.size.per.node=128000000;
SET mapred.min.split.size.per.rack=128000000;
SET hive.exec.reducers.bytes.per.reducer = 128000000;
SET hive.merge.mapfiles=true;
SET hive.merge.mapredfiles= true;
SET hive.input.format=org.apache.hadoop.hive.ql.io.CombineHiveInputFormat;
SET hive.merge.size.per.task=256000000;
SET hive.exec.parallel = true; 

INSERT OVERWRITE TABLE  dw_proj.cat_rank_tmp
SELECT
m.cat_id, 
m.cat_name,
m.parent_id,
m.node1, --一级分类
m.node2, --二级分类
m.node3, --三级分类
m.node4,  --四级分类
CASE WHEN m.cat_id=m.node1 THEN '1'
     WHEN m.cat_id=m.node2 THEN '2'
     WHEN m.cat_id=m.node3 THEN '3'
     WHEN m.cat_id=m.node4 THEN '4'
ELSE '' END as cat_rank
FROM
(
SELECT 
	cat_id, 
	cat_name,
    parent_id,
	split(d.NODE,',')[0] node1, --一级分类
	split(d.NODE,',')[1] node2, --二级分类
	split(d.NODE,',')[2] node3, --三级分类
	split(d.NODE,',')[3] node4  --四级分类
FROM 
    stg.zaful_eload_category d
) m
;




--找到所有分类，和该分类下的所有子分类
INSERT OVERWRITE TABLE dw_proj.zaful_cat_info 
select n.node1 as cat_id,concat_ws(',',collect_set(n.cat)) as cat_all
from (
select m.cat,m.node1 from 
(
select p1.cat,p1.node1  from
(
select o1.node1 as cat,o1.node1 FROM dw_proj.cat_rank_tmp o1
join (select distinct(node1) from  dw_proj.cat_rank_tmp where cat_rank='1') x1 on o1.node1=x1.node1
) p1
union all 
select p2.cat,p2.node1  from
(
select o2.node2 as cat,o2.node1 FROM dw_proj.cat_rank_tmp o2
join (select distinct(node1) from  dw_proj.cat_rank_tmp where cat_rank='1') x2 on o2.node1=x2.node1
) p2
union all 
select p3.cat,p3.node1  from
(
select o3.node3 as cat,o3.node1 FROM dw_proj.cat_rank_tmp o3
join (select distinct(node1) from  dw_proj.cat_rank_tmp where cat_rank='1') x3 on o3.node1=x3.node1
) p3
union all 
select p4.cat,p4.node1  from
(
select o4.node4 as cat,o4.node1 FROM dw_proj.cat_rank_tmp o4
join (select distinct(node1) from  dw_proj.cat_rank_tmp where cat_rank='1') x4 on o4.node1=x4.node1
) p4
) m
group by m.cat,m.node1
)n
group by n.node1

union all

select n.node2 as cat_id,concat_ws(',',collect_set(n.cat)) as cat_all
from (
select m.cat,m.node2 from 
(
select p2.cat,p2.node2  from
(
select o2.node2 as cat,o2.node2 FROM dw_proj.cat_rank_tmp o2
join (select distinct(node2) from  dw_proj.cat_rank_tmp where cat_rank='2') x2 on o2.node2=x2.node2
) p2
union all 
select p3.cat,p3.node2  from
(
select o3.node3 as cat,o3.node2 FROM dw_proj.cat_rank_tmp o3
join (select distinct(node2) from  dw_proj.cat_rank_tmp where cat_rank='2') x3 on o3.node2=x3.node2
) p3
union all 
select p4.cat,p4.node2  from
(
select o4.node4 as cat,o4.node2 FROM dw_proj.cat_rank_tmp o4
join (select distinct(node2) from  dw_proj.cat_rank_tmp where cat_rank='2') x4 on o4.node2=x4.node2
) p4
) m
group by m.cat,m.node2
)n
group by n.node2

union all

select n.node3 as cat_id,concat_ws(',',collect_set(n.cat)) as cat_all
from (
select m.cat,m.node3 from 
(
select p3.cat,p3.node3  from
(
select o3.node3 as cat,o3.node3 FROM dw_proj.cat_rank_tmp o3
join (select distinct(node3) from  dw_proj.cat_rank_tmp where cat_rank='3') x3 on o3.node3=x3.node3
) p3
union all 
select p4.cat,p4.node3  from
(
select o4.node4 as cat,o4.node3 FROM dw_proj.cat_rank_tmp o4
join (select distinct(node3) from  dw_proj.cat_rank_tmp where cat_rank='3') x4 on o4.node3=x4.node3
) p4
) m
group by m.cat,m.node3
)n
group by n.node3

union all

select n.node4 as cat_id,concat_ws(',',collect_set(n.cat)) as cat_all
from (
select m.cat,m.node4 from 
(
select p4.cat,p4.node4  from
(
select o4.node4 as cat,o4.node4 FROM dw_proj.cat_rank_tmp o4
join (select distinct(node4) from  dw_proj.cat_rank_tmp where cat_rank='4') x4 on o4.node4=x4.node4
) p4
) m
group by m.cat,m.node4
)n
group by n.node4

union all

select n.cat_id,
concat_ws(',',collect_set(n.cat)) as cat_all
from
(    
    select 'All' as cat_id,  cat_id as cat from dw_proj.cat_rank_tmp
) n
group by 
n.cat_id
;


INSERT OVERWRITE TABLE dw_proj.zaful_cat_name_info 
select concat_ws('-',b.cat_id,b.cat_name) as cat_id,
cat_all
from dw_proj.zaful_cat_info a 
join dw_proj.cat_rank_tmp b
on a.cat_id=b.cat_id
union all
select cat_id,cat_all from dw_proj.zaful_cat_info
where cat_id='All'
; 