CREATE TABLE `dm_rca.dm_rca_train_sample_all_shucang_v2_filter_by_user`(
  `msg` string COMMENT '用户浏览json')
COMMENT '添加全站浏览json表'
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
  'viewfs://AutoLfCluster/team/dm_rca/hive_db/tmp/dm_rca_train_sample_all_shucang_v2_filter_by_user'
TBLPROPERTIES (
  'transient_lastDdlTime'='1599743888')
;
