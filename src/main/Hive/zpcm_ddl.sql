 CREATE EXTERNAL TABLE `rdz_la_zpcmpf_test`(              
    `hist_id` decimal(20,0) COMMENT 'from deserializer',   
    `zactcode` string COMMENT 'from deserializer',    
    `billfreq` string COMMENT 'from deserializer',    
    `crtable` string COMMENT 'from deserializer',     
    `cnttype` string COMMENT 'from deserializer',     
    `zplandesc` string COMMENT 'from deserializer',   
    `etl_datatime` timestamp COMMENT 'from deserializer',   
    `etl_datatype` string COMMENT 'from deserializer',   
    `dt_datatime` timestamp COMMENT 'from deserializer',   
    `srcebus` string COMMENT 'from deserializer',     
    `strtdt` decimal(16,0) COMMENT 'from deserializer',   
    `zsellterdt` decimal(16,0) COMMENT 'from deserializer',   
    `zterrendt` decimal(16,0) COMMENT 'from deserializer',   
    `effectdate` decimal(16,0) COMMENT 'from deserializer',   
    `trandate` decimal(16,0) COMMENT 'from deserializer',   
    `znwbkfld` string COMMENT 'from deserializer',    
    `actdesc` string COMMENT 'from deserializer'
    )
  ROW FORMAT SERDE                                    
    'org.apache.hadoop.hive.contrib.serde2.MultiDelimitSerDe'   
  WITH SERDEPROPERTIES (                              
    'field.delim'='    ',                             
    'serialization.encoding'='GBK')                   
  STORED AS INPUTFORMAT                               
    'org.apache.hadoop.mapred.TextInputFormat'        
  OUTPUTFORMAT                                        
    'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'  
  LOCATION                                            
    'hdfs://cnszqldlkap02.alico.corp:8020/warehouse/tablespace/external/hive/rt_sync_tmp.db/rdz_la_zpcmpf_test' 