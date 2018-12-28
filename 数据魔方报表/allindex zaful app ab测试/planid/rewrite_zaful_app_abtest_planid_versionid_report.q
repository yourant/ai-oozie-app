select
  c.exp_num,
  d.sku_uv,
  e.click_num,
  f.click_uv,
  e.click_num / c.exp_num,
  f.click_uv / d.sku_uv,
  g.cart_num,
  h.cart_uv,
  g.cart_num / c.exp_num,
  h.cart_uv / d.sku_uv,
  i.order_sku_num,
  j.order_uv,
  i.order_sku_num / c.exp_num,
  j.order_uv / d.sku_uv,
  k.purchase_num,
  l.pay_uv,
  m.pay_amount,
  k.purchase_num / c.exp_num,
  l.pay_uv / d.sku_uv,
  n.collect_uv,
  o.collect_num,
  'android',
  'recommend productdetail',
  c.planid,
  c.versionid
from   
(select exp_num, planid,versionid,add_time from dw_zaful_recommend.rewrite_zaful_app_abtest_allindex_exp_num_tmp where af_inner_mediasource='recommend productdetail' and platform='android' and planid is null) c 
left join 
(select sku_uv, planid,versionid,add_time from dw_zaful_recommend.rewrite_zaful_app_abtest_allindex_sku_uv_tmp where af_inner_mediasource='recommend productdetail' and platform='android' and planid is null) d 
on c.add_time=d.add_time 
left join 
(select click_num,planid,versionid, add_time from dw_zaful_recommend.rewrite_zaful_app_abtest_allindex_click_num_tmp where af_inner_mediasource='recommend productdetail' and platform='android' and planid is null) e 
on c.add_time=e.add_time 
left join 
(select click_uv, planid,versionid,add_time from dw_zaful_recommend.rewrite_zaful_app_abtest_allindex_click_uv_tmp where af_inner_mediasource='recommend productdetail' and platform='android' and planid is null) f 
on c.add_time=f.add_time 
left join 
(select cart_num,planid,versionid, add_time from dw_zaful_recommend.rewrite_zaful_app_abtest_allindex_cart_num_tmp where af_inner_mediasource='recommend productdetail' and platform='android' and  planid is null) g 
on c.add_time=g.add_time 
left join 
(select cart_uv,planid,versionid, add_time from dw_zaful_recommend.rewrite_zaful_app_abtest_allindex_cart_uv_tmp where af_inner_mediasource='recommend productdetail' and platform='android' and planid is null) h 
on c.add_time=h.add_time 
left join 
(select order_sku_num, planid,versionid,add_time from dw_zaful_recommend.rewrite_zaful_app_abtest_allindex_order_sku_num_tmp where af_inner_mediasource='recommend productdetail' and platform='android' and planid is null) i 
on c.add_time=i.add_time 
left join 
(select order_uv,planid,versionid, add_time from dw_zaful_recommend.rewrite_zaful_app_abtest_allindex_order_uv_tmp where af_inner_mediasource='recommend productdetail' and platform='android' and planid is null) j 
on c.add_time=j.add_time 
left join 
(select purchase_num, planid,versionid,add_time from dw_zaful_recommend.rewrite_zaful_app_abtest_allindex_purchase_num_tmp where af_inner_mediasource='recommend productdetail' and platform='android' and planid is null) k 
on c.add_time=k.add_time 
left join 
(select pay_uv, planid,versionid,add_time from dw_zaful_recommend.rewrite_zaful_app_abtest_allindex_pay_uv_tmp where af_inner_mediasource='recommend productdetail' and platform='android' and planid is null) l 
on c.add_time=l.add_time 
left join 
(select pay_amount,planid,versionid, add_time from dw_zaful_recommend.rewrite_zaful_app_abtest_allindex_pay_amount_tmp where af_inner_mediasource='recommend productdetail' and platform='android' and planid is null) m 
on c.add_time=m.add_time 
left join 
(select collect_uv, planid,versionid,add_time from dw_zaful_recommend.rewrite_zaful_app_abtest_allindex_collect_uv_tmp where af_inner_mediasource='recommend productdetail' and platform='android' and planid is null) n 
on c.add_time=n.add_time 
left join 
(select collect_num, planid,versionid,add_time from dw_zaful_recommend.rewrite_zaful_app_abtest_allindex_collect_num_tmp where af_inner_mediasource='recommend productdetail' and platform='android' and planid is null) o 
on c.add_time=o.add_time 
union all
select
  c.exp_num,
  d.sku_uv,
  e.click_num,
  f.click_uv,
  e.click_num / c.exp_num,
  f.click_uv / d.sku_uv,
  g.cart_num,
  h.cart_uv,
  g.cart_num / c.exp_num,
  h.cart_uv / d.sku_uv,
  i.order_sku_num,
  j.order_uv,
  i.order_sku_num / c.exp_num,
  j.order_uv / d.sku_uv,
  k.purchase_num,
  l.pay_uv,
  m.pay_amount,
  k.purchase_num / c.exp_num,
  l.pay_uv / d.sku_uv,
  n.collect_uv,
  o.collect_num,
  'android',
  'recommend_homepage',
  c.planid,
  c.versionid
from   
(select exp_num, planid,versionid,add_time from dw_zaful_recommend.rewrite_zaful_app_abtest_allindex_exp_num_tmp where af_inner_mediasource='recommend_homepage' and platform='android' and planid is null) c 
left join 
(select sku_uv, planid,versionid,add_time from dw_zaful_recommend.rewrite_zaful_app_abtest_allindex_sku_uv_tmp where af_inner_mediasource='recommend_homepage' and platform='android' and planid is null) d 
on c.add_time=d.add_time 
left join 
(select click_num,planid,versionid, add_time from dw_zaful_recommend.rewrite_zaful_app_abtest_allindex_click_num_tmp where af_inner_mediasource='recommend_homepage' and platform='android' and planid is null) e 
on c.add_time=e.add_time 
left join 
(select click_uv, planid,versionid,add_time from dw_zaful_recommend.rewrite_zaful_app_abtest_allindex_click_uv_tmp where af_inner_mediasource='recommend_homepage' and platform='android' and planid is null) f 
on c.add_time=f.add_time 
left join 
(select cart_num,planid,versionid, add_time from dw_zaful_recommend.rewrite_zaful_app_abtest_allindex_cart_num_tmp where af_inner_mediasource='recommend_homepage' and platform='android' and planid is null) g 
on c.add_time=g.add_time
left join 
(select cart_uv,planid,versionid, add_time from dw_zaful_recommend.rewrite_zaful_app_abtest_allindex_cart_uv_tmp where af_inner_mediasource='recommend_homepage' and platform='android' and planid is null) h 
on c.add_time=h.add_time 
left join 
(select order_sku_num, planid,versionid,add_time from dw_zaful_recommend.rewrite_zaful_app_abtest_allindex_order_sku_num_tmp where af_inner_mediasource='recommend_homepage' and platform='android' and planid is null) i 
on c.add_time=i.add_time 
left join 
(select order_uv,planid,versionid, add_time from dw_zaful_recommend.rewrite_zaful_app_abtest_allindex_order_uv_tmp where af_inner_mediasource='recommend_homepage' and platform='android' and planid is null) j 
on c.add_time=j.add_time 
left join 
(select purchase_num, planid,versionid,add_time from dw_zaful_recommend.rewrite_zaful_app_abtest_allindex_purchase_num_tmp where af_inner_mediasource='recommend_homepage' and platform='android' and planid is null) k 
on c.add_time=k.add_time 
left join 
(select pay_uv, planid,versionid,add_time from dw_zaful_recommend.rewrite_zaful_app_abtest_allindex_pay_uv_tmp where af_inner_mediasource='recommend_homepage' and platform='android' and planid is null) l 
on c.add_time=l.add_time 
left join 
(select pay_amount,planid,versionid, add_time from dw_zaful_recommend.rewrite_zaful_app_abtest_allindex_pay_amount_tmp where af_inner_mediasource='recommend_homepage' and platform='android' and planid is null) m 
on c.add_time=m.add_time 
left join 
(select collect_uv, planid,versionid,add_time from dw_zaful_recommend.rewrite_zaful_app_abtest_allindex_collect_uv_tmp where af_inner_mediasource='recommend_homepage' and platform='android' and planid is null) n 
on c.add_time=n.add_time 
left join 
(select collect_num, planid,versionid,add_time from dw_zaful_recommend.rewrite_zaful_app_abtest_allindex_collect_num_tmp where af_inner_mediasource='recommend_homepage' and platform='android' and planid is null) o 
on c.add_time=o.add_time 
union all
select
  c.exp_num,
  d.sku_uv,
  e.click_num,
  f.click_uv,
  e.click_num / c.exp_num,
  f.click_uv / d.sku_uv,
  g.cart_num,
  h.cart_uv,
  g.cart_num / c.exp_num,
  h.cart_uv / d.sku_uv,
  i.order_sku_num,
  j.order_uv,
  i.order_sku_num / c.exp_num,
  j.order_uv / d.sku_uv,
  k.purchase_num,
  l.pay_uv,
  m.pay_amount,
  k.purchase_num / c.exp_num,
  l.pay_uv / d.sku_uv,
  n.collect_uv,
  o.collect_num,
  'ios',
  'recommend productdetail',
  c.planid,
  c.versionid
from   
(select exp_num, planid,versionid,add_time from dw_zaful_recommend.rewrite_zaful_app_abtest_allindex_exp_num_tmp where af_inner_mediasource='recommend productdetail' and platform='ios' and planid is null) c 
left join 
(select sku_uv, planid,versionid,add_time from dw_zaful_recommend.rewrite_zaful_app_abtest_allindex_sku_uv_tmp where af_inner_mediasource='recommend productdetail' and platform='ios' and planid is null) d 
on c.add_time=d.add_time
left join 
(select click_num,planid,versionid, add_time from dw_zaful_recommend.rewrite_zaful_app_abtest_allindex_click_num_tmp where af_inner_mediasource='recommend productdetail' and platform='ios' and planid is null) e 
on c.add_time=e.add_time 
left join 
(select click_uv, planid,versionid,add_time from dw_zaful_recommend.rewrite_zaful_app_abtest_allindex_click_uv_tmp where af_inner_mediasource='recommend productdetail' and platform='ios' and planid is null) f 
on c.add_time=f.add_time 
left join 
(select cart_num,planid,versionid, add_time from dw_zaful_recommend.rewrite_zaful_app_abtest_allindex_cart_num_tmp where af_inner_mediasource='recommend productdetail' and platform='ios' and planid is null) g 
on c.add_time=g.add_time 
left join 
(select cart_uv,planid,versionid, add_time from dw_zaful_recommend.rewrite_zaful_app_abtest_allindex_cart_uv_tmp where af_inner_mediasource='recommend productdetail' and platform='ios' and planid is null) h 
on c.add_time=h.add_time 
left join 
(select order_sku_num, planid,versionid,add_time from dw_zaful_recommend.rewrite_zaful_app_abtest_allindex_order_sku_num_tmp where af_inner_mediasource='recommend productdetail' and platform='ios' and planid is null) i 
on c.add_time=i.add_time 
left join 
(select order_uv,planid,versionid, add_time from dw_zaful_recommend.rewrite_zaful_app_abtest_allindex_order_uv_tmp where af_inner_mediasource='recommend productdetail' and platform='ios' and planid is null) j 
on c.add_time=j.add_time 
left join 
(select purchase_num, planid,versionid,add_time from dw_zaful_recommend.rewrite_zaful_app_abtest_allindex_purchase_num_tmp where af_inner_mediasource='recommend productdetail' and platform='ios' and planid is null) k 
on c.add_time=k.add_time 
left join 
(select pay_uv, planid,versionid,add_time from dw_zaful_recommend.rewrite_zaful_app_abtest_allindex_pay_uv_tmp where af_inner_mediasource='recommend productdetail' and platform='ios' and planid is null) l 
on c.add_time=l.add_time 
left join 
(select pay_amount,planid,versionid, add_time from dw_zaful_recommend.rewrite_zaful_app_abtest_allindex_pay_amount_tmp where af_inner_mediasource='recommend productdetail' and platform='ios' and planid is null) m 
on c.add_time=m.add_time 
left join 
(select collect_uv, planid,versionid,add_time from dw_zaful_recommend.rewrite_zaful_app_abtest_allindex_collect_uv_tmp where af_inner_mediasource='recommend productdetail' and platform='ios' and planid is null) n 
on c.add_time=n.add_time 
left join 
(select collect_num, planid,versionid,add_time from dw_zaful_recommend.rewrite_zaful_app_abtest_allindex_collect_num_tmp where af_inner_mediasource='recommend productdetail' and platform='ios' and planid is null) o 
on c.add_time=o.add_time 
union all
select
  c.exp_num,
  d.sku_uv,
  e.click_num,
  f.click_uv,
  e.click_num / c.exp_num,
  f.click_uv / d.sku_uv,
  g.cart_num,
  h.cart_uv,
  g.cart_num / c.exp_num,
  h.cart_uv / d.sku_uv,
  i.order_sku_num,
  j.order_uv,
  i.order_sku_num / c.exp_num,
  j.order_uv / d.sku_uv,
  k.purchase_num,
  l.pay_uv,
  m.pay_amount,
  k.purchase_num / c.exp_num,
  l.pay_uv / d.sku_uv,
  n.collect_uv,
  o.collect_num,
  'ios',
  'recommend_homepage',
  c.planid,
  c.versionid
from   
(select exp_num, planid,versionid,add_time from dw_zaful_recommend.rewrite_zaful_app_abtest_allindex_exp_num_tmp where af_inner_mediasource='recommend_homepage' and platform='ios' and planid is null) c 
left join 
(select sku_uv, planid,versionid,add_time from dw_zaful_recommend.rewrite_zaful_app_abtest_allindex_sku_uv_tmp where af_inner_mediasource='recommend_homepage' and platform='ios' and planid is null) d 
on c.add_time=d.add_time 
left join 
(select click_num,planid,versionid, add_time from dw_zaful_recommend.rewrite_zaful_app_abtest_allindex_click_num_tmp where af_inner_mediasource='recommend_homepage' and platform='ios' and planid is null) e 
on c.add_time=e.add_time 
left join 
(select click_uv, planid,versionid,add_time from dw_zaful_recommend.rewrite_zaful_app_abtest_allindex_click_uv_tmp where af_inner_mediasource='recommend_homepage' and platform='ios' and planid is null) f 
on c.add_time=f.add_time 
left join 
(select cart_num,planid,versionid, add_time from dw_zaful_recommend.rewrite_zaful_app_abtest_allindex_cart_num_tmp where af_inner_mediasource='recommend_homepage' and platform='ios' and planid is null) g 
on c.add_time=g.add_time 
left join 
(select cart_uv,planid,versionid, add_time from dw_zaful_recommend.rewrite_zaful_app_abtest_allindex_cart_uv_tmp where af_inner_mediasource='recommend_homepage' and platform='ios' and planid is null) h 
on c.add_time=h.add_time 
left join 
(select order_sku_num, planid,versionid,add_time from dw_zaful_recommend.rewrite_zaful_app_abtest_allindex_order_sku_num_tmp where af_inner_mediasource='recommend_homepage' and platform='ios' and planid is null) i 
on c.add_time=i.add_time 
left join 
(select order_uv,planid,versionid, add_time from dw_zaful_recommend.rewrite_zaful_app_abtest_allindex_order_uv_tmp where af_inner_mediasource='recommend_homepage' and platform='ios' and planid is null) j 
on c.add_time=j.add_time 
left join 
(select purchase_num, planid,versionid,add_time from dw_zaful_recommend.rewrite_zaful_app_abtest_allindex_purchase_num_tmp where af_inner_mediasource='recommend_homepage' and platform='ios' and planid is null) k 
on c.add_time=k.add_time 
left join 
(select pay_uv, planid,versionid,add_time from dw_zaful_recommend.rewrite_zaful_app_abtest_allindex_pay_uv_tmp where af_inner_mediasource='recommend_homepage' and platform='ios' and planid is null) l 
on c.add_time=l.add_time 
left join 
(select pay_amount,planid,versionid, add_time from dw_zaful_recommend.rewrite_zaful_app_abtest_allindex_pay_amount_tmp where af_inner_mediasource='recommend_homepage' and platform='ios' and planid is null) m 
on c.add_time=m.add_time 
left join 
(select collect_uv, planid,versionid,add_time from dw_zaful_recommend.rewrite_zaful_app_abtest_allindex_collect_uv_tmp where af_inner_mediasource='recommend_homepage' and platform='ios' and planid is null) n 
on c.add_time=n.add_time 
left join 
(select collect_num, planid,versionid,add_time from dw_zaful_recommend.rewrite_zaful_app_abtest_allindex_collect_num_tmp where af_inner_mediasource='recommend_homepage' and platform='ios' and planid is null) o 
on c.add_time=o.add_time 