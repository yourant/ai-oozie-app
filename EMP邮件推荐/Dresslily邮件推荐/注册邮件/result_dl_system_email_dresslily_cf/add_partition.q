

Alter TABLE dw_dresslily_recommend.result_dl_system_email_dresslily_cf ADD IF NOT EXISTS PARTITION  (year=${YEAR},month=${MONTH},day=${DAY}) location  '/user/hive/warehouse/dw_dresslily_recommend.db/result_dl_system_email_dresslily_cf/year=${YEAR}/month=${MONTH}/day=${DAY}';
