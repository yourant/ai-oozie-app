CREATE TABLE IF NOT EXISTS dw_zaful_recommend.zaful_list_is_more_color_mid (
	          
	goods_id int COMMENT '',
 
	goods_sn string COMMENT '��Ʒid',

	group_goods_id string COMMENT 'ͬһ��Ʒ��ͬ���ͳһID��',       
        
	is_more_color  string COMMENT '����ɫ y�� n��'
) 

COMMENT '��Ʒ�Ƿ�Ϊ����ɫ�м��' 
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' 

LINES TERMINATED BY '\n' 

STORED AS TEXTFILE;


insert overwrite table dw_zaful_recommend.zaful_list_is_more_color_mid 
select 
	t4.goods_id,
	t4.goods_sn,
	t4.group_goods_id,
	t3.is_more_color      
from 
	stg.zaful_eload_goods t4
join 
	(select 
		case when t2.num>1 then '1'
       		else '0' end as is_more_color,
		t2.group_goods_id
	from 
		(select 
			count(t1.group_goods_id) as num,
			t1.group_goods_id 
		from 
			(select 
		
				t0.goods_id,
				t.group_goods_id
      
			from 
		
				stg.zaful_eload_goods_attr  t0
      
			join 
			
				stg.zaful_eload_goods t 
	
			on
		
				t0.attr_id=8 
		
				and t0.goods_id=t.goods_id  
			group by 
				t0.goods_id,t.group_goods_id
			) t1 
		group by t1.group_goods_id 
		) t2
	) t3 
on
	t4.group_goods_id=t3.group_goods_id 
group by 
	t4.goods_id,
	t4.goods_sn,
	t4.group_goods_id,
	t3.is_more_color;



CREATE TABLE IF NOT EXISTS dw_zaful_recommend.zaful_list_reivew_count_mid (
	          
	goods_sn string COMMENT '��Ʒid',

	review_count string COMMENT '������',       
        
	avg_rate  string COMMENT '����'
) 

COMMENT '��Ʒ�����м��' 
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' 

LINES TERMINATED BY '\n' 

STORED AS TEXTFILE;

insert overwrite table dw_zaful_recommend.zaful_list_reivew_count_mid 
select  
	a.goods_sn,
	b.review_count,
	b.avg_rate 
from 
	stg.zaful_eload_goods a 
join    stg.zaful_eload_goods_extend  b 
on 
	a.goods_id=b.goods_id 
group by 
	a.goods_sn,
	b.review_count,
	b.avg_rate ;


CREATE TABLE IF NOT EXISTS dw_zaful_recommend.zaful_list_source_sku_fact  
 (
	    
	goods_sn  string  COMMENT '��ƷSKU',
        
	is_new int COMMENT '��Ʒ��־',
        
	node1 string COMMENT 'һ������',
 
	node2 string COMMENT '��������',
 
	node3 string COMMENT '��������',
 
	node4 string COMMENT '�ļ�����',
 
        color_code string COMMENT '��ɫ��',
	shop_price decimal(12,2) COMMENT '�����ۼ�',
	is_more_color  string COMMENT '����ɫ y�� n��',
	is_priority_dispaching tinyint COMMENT '24Сʱ����',
	add_time int COMMENT '',
	review_count string COMMENT '������',       
        
	avg_rate  string COMMENT '����') 

COMMENT '�б�ҳ��' 
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' 

LINES TERMINATED BY '\n' 

STORED AS TEXTFILE;

insert overwrite table dw_zaful_recommend.zaful_list_source_sku_fact
select 
	t0.goods_sn,
	t0.is_new,
	t0.node1,
	t0.node2,
	t0.node3,
	t0.node4,
	t0.color_code,
	t6.shop_price,
	t4.is_more_color,
	t5.is_24h_ship,
	t6.add_time,
        t7.review_count,
	t7.avg_rate  
from 
	dw_zaful_recommend.zaful_list_source_sku t0 
left join 
	(select 
		goods_sn,
		is_more_color 
	from
		dw_zaful_recommend.zaful_list_is_more_color_mid 
	group by 
		goods_sn,
		is_more_color 
	) t4 
on 
	t0.goods_sn=t4.goods_sn 
left join 
	(select 
		a.is_24h_ship,
		b.goods_sn
	from 
		stg.zaful_eload_goods_extend a 
	join 	
		stg.zaful_eload_goods b 
	on 
		a.goods_id=b.goods_id 
	group by 
		a.is_24h_ship,
		b.goods_sn 
	) t5 
on 
	t0.goods_sn=t5.goods_sn  
left join 
	(select 
		goods_sn,
		add_time,
		shop_price   
	from 
		stg.zaful_eload_goods 
	group by 
		goods_sn,
		add_time,
		shop_price   
	) t6 
on 
	t0.goods_sn=t6.goods_sn	
left join 
	dw_zaful_recommend.zaful_list_reivew_count_mid t7 
on 
	t0.goods_sn=t7.goods_sn 
group by 
	t0.goods_sn,
	t0.is_new,
	t0.node1,
	t0.node2,
	t0.node3,
	t0.node4,
	t0.color_code,
        t6.shop_price,
	t4.is_more_color,
	t5.is_24h_ship,
	t6.add_time,
	t7.review_count,
	t7.avg_rate; 

		
	





