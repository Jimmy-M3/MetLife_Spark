 CREATE EXTERNAL TABLE `login_data`(
    `logtime` string COMMENT 'from deserializer',
    `account_id` string COMMENT 'from deserializer',
    `ip` string COMMENT 'from deserializer')
  PARTITIONED BY (
    `date_time` string)
  ROW FORMAT SERDE
    'org.apache.hadoop.hive.serde2.OpenCSVSerde'
  STORED AS INPUTFORMAT
    'org.apache.hadoop.mapred.TextInputFormat'
  OUTPUTFORMAT
    'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
  LOCATION
    'hdfs://cnszqldlkap02.alico.corp:8020/warehouse/tablespace/external/hive/rt_sync_tmp.db/login_data'
  TBLPROPERTIES (
    'bucketing_version'='2',
    'discover.partitions'='true')