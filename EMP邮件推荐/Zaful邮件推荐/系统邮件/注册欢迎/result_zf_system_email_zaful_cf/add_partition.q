

Alter TABLE dw_zaful_recommend.result_zf_system_email_zaful_cf ADD IF NOT EXISTS PARTITION  (year=${YEAR},month=${MONTH},day=${DAY}) location  '/user/hive/warehouse/dw_zaful_recommend.db/result_zf_system_email_zaful_cf/year=${YEAR}/month=${MONTH}/day=${DAY}';
