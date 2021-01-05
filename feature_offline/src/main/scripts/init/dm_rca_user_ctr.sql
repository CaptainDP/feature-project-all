CREATE TABLE `dm_rca.dm_rca_user_ctr`(
  `device_id` string COMMENT '用户设备编号',
  `ctr` string COMMENT '用户点击率'
  )
COMMENT '用户点击率表'
PARTITIONED BY (
  `dt` string COMMENT '日期分区字段')
ROW FORMAT SERDE
  'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
WITH SERDEPROPERTIES (
  'field.delim'='\t',
  'line.delim'='\n',
  'serialization.format'='\t')
STORED AS INPUTFORMAT
  'org.apache.hadoop.mapred.TextInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
  'viewfs://AutoLfCluster/team/dm_rca/hive_db/tmp/dm_rca_user_ctr'
TBLPROPERTIES (
  'transient_lastDdlTime'='1599743888')
;
