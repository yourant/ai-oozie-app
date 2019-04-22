

Alter TABLE dw_rg_recommend.result_rg_system_email_rg_cf ADD IF NOT EXISTS PARTITION  (year=${YEAR},month=${MONTH},day=${DAY}) location  '/user/hive/warehouse/dw_rg_recommend.db/result_rg_system_email_rg_cf/year=${YEAR}/month=${MONTH}/day=${DAY}';
