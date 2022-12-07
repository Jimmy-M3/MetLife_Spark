package com.datalake

import org.apache.spark.sql.SparkSession

object StreamingKafka2Hive_pt {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("Kafka Test")
      .getOrCreate()
    val bootstrap = "cnszqldlkap03.alico.corp:6667"
    val topic = "test"

    val df = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", bootstrap)
      .option("subscribe", topic)
      .option("kafka.security.protocol", "SASL_PLAINTEXT")
      .option("kafka.sasl.kerberos.service.name", "kafka")
      .option("kafka.sasl.mechanism", "GSSAPI")
      .load()

    var df2 = df.selectExpr("cast(value as String)", "timestamp", "topic")

    df2.writeStream
      .format("csv")
      .option("checkpointLocation","hdfs://cnszqldlkap02.alico.corp:8020/tmp/ckp")
      .option("path","hdfs://cnszqldlkap02.alico.corp:8020/tmp/kafka-test.csv")
      .outputMode("append")
//      .trigger(Trigger.ProcessingTime(1000))
      .start()
      .awaitTermination()

    spark.stop()
  }
//  case class DataSchema(
//                       hist_id:Int,
//                       zactcode:String,
//                       billfreq:String,
//                       crtable:String,
//                       cnttype:String,
//                       zplandesc:String
//                       )
}
//hist_id decimal(20,0) ,
//zactcode varchar(100) ,
//billfreq varchar(100) ,
//crtable varchar(100) ,
//cnttype varchar(100) ,
//zplandesc varchar(100),