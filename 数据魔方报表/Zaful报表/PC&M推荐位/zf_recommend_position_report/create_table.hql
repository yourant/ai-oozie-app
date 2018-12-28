--输出结果表
CREATE TABLE IF NOT EXISTS dw_zaful_recommend.zaful_recommend_position_report(
pv                        INT            COMMENT "页面PV",
uv                        INT            COMMENT "页面UV",
exp_num                   INT            COMMENT "商品曝光数",
click_num                 INT            COMMENT "商品点击数",
exp_click_ratio           decimal(10,4)  COMMENT "曝光点击率",
cart_num                  INT            COMMENT "商品加购数",
cart_ratio                decimal(10,4)  COMMENT "加购率",
order_num                 INT            COMMENT "商品下单数",   
conversion_ratio          decimal(10,4)  COMMENT "下单转化率",
purchase_num              INT            COMMENT "支付订单数",  
pay_amount                decimal(10,4)  COMMENT "购买金额",
purchase_ratio            decimal(10,4)  COMMENT "购买转化率",
total_conversion_ratio    decimal(10,4)  COMMENT "总体转化率",
collect_num               INT            COMMENT "商品收藏数",
platform                  STRING         COMMENT "平台",
recommend_position        STRING         COMMENT "推荐位编号",
position_name             STRING         COMMENT "推荐位名称",
glb_dc                    STRING         COMMENT "语言站",
country                   STRING         COMMENT "国家"
)
COMMENT '推荐位数据报表'
PARTITIONED BY (add_time STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\u0001' LINES TERMINATED BY '\n' STORED AS TEXTFILE
;


--页面PV 表zf_pc_event_info
--条件：glb_plf=pc,glb_t=ie,glb_b=a,glb_ubcta为空，计算glb_t的数量
DROP TABLE IF EXISTS dw_zaful_recommend.report_pv_tmp;
CREATE TABLE IF NOT EXISTS dw_zaful_recommend.report_pv_tmp(
pv                        INT            COMMENT "页面PV",
glb_plf                   STRING         COMMENT "平台",
glb_b                     STRING         COMMENT "页面大类",
country                   STRING         COMMENT "国家",
glb_dc                    STRING         COMMENT "语言站",
add_time                  STRING         COMMENT "日期"
)
COMMENT "推荐位报表页面PV中间表"
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\u0001' LINES TERMINATED BY '\n' STORED AS TEXTFILE;

--页面UV	表zf_pc_event_info
--条件：glb_plf=pc,glb_b=a，计算glb_od去重后的数量
DROP TABLE IF EXISTS dw_zaful_recommend.report_uv_tmp;
CREATE TABLE IF NOT EXISTS dw_zaful_recommend.report_uv_tmp(
uv                        INT            COMMENT "页面UV",
glb_plf                   STRING         COMMENT "平台",
glb_b                     STRING         COMMENT "页面大类",
country                   STRING         COMMENT "国家",
glb_dc                    STRING         COMMENT "语言站",
add_time                  STRING         COMMENT "日期"
)
COMMENT "推荐位报表页面UV中间表"
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\u0001' LINES TERMINATED BY '\n' STORED AS TEXTFILE;

--商品曝光数	表zf_pc_event_info
--条件：glb_b=a,glb_t=ie,glb_pm=mr,glb_plf=pc，glb_ubcta中jason字段mdlc=T_1，计算glb_ubcta中jason字段sku的数量
DROP TABLE IF EXISTS dw_zaful_recommend.report_exp_tmp;
CREATE TABLE IF NOT EXISTS dw_zaful_recommend.report_exp_tmp(
exp_num                   INT            COMMENT "商品曝光数",
glb_plf                   STRING         COMMENT "平台",
glb_b                     STRING         COMMENT "页面大类",
mdlc                      STRING         COMMENT "推荐位编号",
country                   STRING         COMMENT "国家",
glb_dc                    STRING         COMMENT "语言站",
add_time                  STRING         COMMENT "日期"
)
COMMENT "推荐位报表商品曝光数中间表"
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\u0001' LINES TERMINATED BY '\n' STORED AS TEXTFILE;

--商品点击数	表zf_pc_event_info
--条件：glb_b=a,glb_t=ic,glb_pm=mr,glb_plf=pc，glb_x=sku，glb_ubcta中jason字段mdlc=T_1,计算glb_ubcta中jason字段sku的数量
DROP TABLE IF EXISTS dw_zaful_recommend.report_click_tmp;
CREATE TABLE IF NOT EXISTS dw_zaful_recommend.report_click_tmp(
click_num                 INT            COMMENT "商品点击数",
glb_plf                   STRING         COMMENT "平台",
glb_b                     STRING         COMMENT "页面大类",
mdlc                      STRING         COMMENT "推荐位编号",
country                   STRING         COMMENT "国家",
glb_dc                    STRING         COMMENT "语言站",
add_time                  STRING         COMMENT "日期"
)
COMMENT "推荐位报表商品点击数中间表"
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\u0001' LINES TERMINATED BY '\n' STORED AS TEXTFILE;

--商品加购数	表zf_pc_event_info
--条件：glb_t=ic,glb_x=ADT,glb_ubcta中jason字段fmd=mr_T_1,glb_plf=pc，计算glb_t的数量
DROP TABLE IF EXISTS dw_zaful_recommend.report_cart_tmp;
CREATE TABLE IF NOT EXISTS dw_zaful_recommend.report_cart_tmp(
cart_num                  INT            COMMENT "商品加购数",
glb_plf                   STRING         COMMENT "平台",
fmd                       STRING         COMMENT "推荐位编号",
country                   STRING         COMMENT "国家",
glb_dc                    STRING         COMMENT "语言站",
add_time                  STRING         COMMENT "日期"
)
COMMENT "推荐位报表商品加购数中间表"
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\u0001' LINES TERMINATED BY '\n' STORED AS TEXTFILE;

--商品下单数	表zf_pc_event_info、zaful_eload_order_info、zaful_eload_order_goods

--中间表report_sku_user_tmp：取sku,user_id
DROP TABLE IF EXISTS dw_zaful_recommend.report_sku_user_country_tmp;
CREATE TABLE IF NOT EXISTS dw_zaful_recommend.report_sku_user_country_tmp(
sku                       STRING         COMMENT "sku",
user_id                   STRING         COMMENT "用户ID",
glb_plf                   STRING         COMMENT "平台",
country                   STRING         COMMENT "国家",
glb_dc                    STRING         COMMENT "语言站",
fmd                       STRING         COMMENT "推荐位编号"
)
COMMENT "推荐位报表user-sku-county中间表"
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\u0001' LINES TERMINATED BY '\n' STORED AS TEXTFILE;

DROP TABLE IF EXISTS dw_zaful_recommend.report_sku_user_tmp;
CREATE TABLE IF NOT EXISTS dw_zaful_recommend.report_sku_user_tmp(
goods_sn                  STRING         COMMENT "sku",
user_id                   STRING         COMMENT "用户ID",
order_status              STRING         COMMENT "订单状态",
pay_amount                decimal(10,2)  COMMENT "购买金额"
)
COMMENT "推荐位报表user-sku中间表"
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\u0001' LINES TERMINATED BY '\n' STORED AS TEXTFILE;

--商品下单数结果表
DROP TABLE IF EXISTS dw_zaful_recommend.report_order_tmp;
CREATE TABLE IF NOT EXISTS dw_zaful_recommend.report_order_tmp(
order_num                 INT            COMMENT "商品下单数",
glb_plf                   STRING         COMMENT "平台",
fmd                       STRING         COMMENT "推荐位编号",
country                   STRING         COMMENT "国家",
glb_dc                    STRING         COMMENT "语言站",
add_time                  STRING         COMMENT "日期"
)
COMMENT "推荐位报表商品下单数中间表"
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\u0001' LINES TERMINATED BY '\n' STORED AS TEXTFILE;

--支付订单数	表zf_pc_event_info、zaful_eload_order_info、zaful_eload_order_goods
DROP TABLE IF EXISTS dw_zaful_recommend.report_purchase_tmp;
CREATE TABLE IF NOT EXISTS dw_zaful_recommend.report_purchase_tmp(
purchase_num              INT            COMMENT "支付订单数",
glb_plf                   STRING         COMMENT "平台",
fmd                       STRING         COMMENT "推荐位编号",
country                   STRING         COMMENT "国家",
glb_dc                    STRING         COMMENT "语言站",
add_time                  STRING         COMMENT "日期"
)
COMMENT "推荐位报表支付订单数中间表"
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\u0001' LINES TERMINATED BY '\n' STORED AS TEXTFILE;

--购买金额	表zf_pc_event_info、zaful_eload_order_info、zaful_eload_order_goods
DROP TABLE IF EXISTS dw_zaful_recommend.report_pay_amount_tmp;
CREATE TABLE IF NOT EXISTS dw_zaful_recommend.report_pay_amount_tmp(
pay_amount                decimal(10,2)  COMMENT "购买金额",
glb_plf                   STRING         COMMENT "平台",
fmd                       STRING         COMMENT "推荐位编号",
country                   STRING         COMMENT "国家",
glb_dc                    STRING         COMMENT "语言站",
add_time                  STRING         COMMENT "日期"
)
COMMENT "推荐位报表购买金额中间表"
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\u0001' LINES TERMINATED BY '\n' STORED AS TEXTFILE;

--商品收藏数	表zf_pc_event_info
DROP TABLE IF EXISTS dw_zaful_recommend.report_collect_tmp;
CREATE TABLE IF NOT EXISTS dw_zaful_recommend.report_collect_tmp(
collect_num               INT            COMMENT "商品收藏数",
glb_plf                   STRING         COMMENT "平台",
fmd                       STRING         COMMENT "推荐位编号",
country                   STRING         COMMENT "国家",
glb_dc                    STRING         COMMENT "语言站",
add_time                  STRING         COMMENT "日期"
)
COMMENT "推荐位报表商品收藏数中间表"
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\u0001' LINES TERMINATED BY '\n' STORED AS TEXTFILE;