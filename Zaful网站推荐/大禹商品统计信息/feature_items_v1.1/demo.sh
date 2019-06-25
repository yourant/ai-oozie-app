#!/bin/sh
#author:cujian
#--创建相关数据表
#清洗浏览日志
today=$(date +%Y%m%d)
yestoday=$(date -d "$today -2day" +%Y%m%d)
day=$(date +%d)
#使用3个月浏览数据
start_time=$(date -d "today -13day" +%Y%m%d)
while true ; do
	start_time=$(date -d "$start_time 1day" +%Y%m%d)
	year_now=$(date -d "$start_time" +%Y)
	month_now=$(date -d "$start_time" +%m)
	day_now=$(date -d "$start_time" +%d)
	
	hive -e"
CREATE TABLE IF NOT EXISTS zaful_recommend.zaful_kkk_test(glb_oi string, log_id string, glb_tm string);
INSERT OVERWRITE TABLE zaful_recommend.zaful_kkk_test
SELECT
  glb_oi,
  log_id,
  glb_tm
FROM
  stg.zf_pc_event_info
WHERE
  year = $year_now
  AND month = $month_now
  AND day = $day_now
  AND glb_t = 'ie'
  AND glb_ubcta != ''
  AND glb_b='b';
  
"

	if [[ $start_time -eq $yestoday ]]; then
		break
	fi

done


