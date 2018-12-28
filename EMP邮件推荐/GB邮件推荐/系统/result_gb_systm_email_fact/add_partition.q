

Alter TABLE dw_gearbest_recommend.result_gb_systm_email_gtq ADD IF NOT EXISTS PARTITION  (year=${YEAR},month=${MONTH},day=${DAY}) location  '/user/hive/warehouse/dw_gearbest_recommend.db/result_gb_systm_email_gtq/year=${YEAR}/month=${MONTH}/day=${DAY}';
