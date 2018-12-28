alter table dw_proj.tb_flow_user add if not exists partition (dt='${ADD_TIME}') location 'hdfs://glbgnameservice/user/hive/warehouse/dw_proj.db/tb_flow_user/dt=${ADD_TIME}';
