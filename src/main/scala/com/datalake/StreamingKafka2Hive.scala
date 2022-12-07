package com.datalake

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, dayofyear, from_json}
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.types.{StringType, StructType}

object StreamingKafka2Hive {
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

    import spark.implicits


    val schema:StructType = new StructType().add("ZACTCODE", StringType, true).add("BILLFREQ", StringType, true)
    val df2 = df.selectExpr("CAST(key AS STRING)", "CAST(topic AS STRING)", "CAST(value AS STRING)","timestamp")
    val df3 = df2.withColumn("value",from_json(col("value"),schema))
    val df4 = df3.withColumn("partitionBy",dayofyear(col("timestamp")))
    val df5 = df4.selectExpr("key","value.*","timestamp")
    val query = df5
        .writeStream.partitionBy("partitionBy")
        .outputMode("append")
        .format("text")
        .option("checkpointLocation","hdfs://cnszqldlkap02.alico.corp:8020/tmp/ckp_pt")
        .option("path","hdfs://cnszqldlkap02.alico.corp:8020/warehouse/tablespace/external/hive/rt_sync_tmp.db/rdz_la_zpcmpf_test_pt")
        .trigger(Trigger.ProcessingTime("2 seconds"))
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