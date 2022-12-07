package com.datalake

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, concat_ws, date_format, dayofyear, from_json}
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.types.{StringType, StructType}

object Kafka_HivePt {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("Kafka Test")
      .getOrCreate()

    val bootstrap = "cnszqldlkap03.alico.corp:6667"
    val topic = "test"

    val df = spark.readStream.format("kafka").option("kafka.bootstrap.servers", bootstrap).option("subscribe", topic).option("kafka.security.protocol", "SASL_PLAINTEXT").option("kafka.sasl.kerberos.service.name", "kafka").option("kafka.sasl.mechanism", "GSSAPI").load()

    val schema:StructType = new StructType().add("ZACTCODE", StringType, true).add("BILLFREQ", StringType, true)
    val df2 = df.selectExpr("CAST(key AS STRING)", "CAST(topic AS STRING)", "CAST(value AS STRING)","timestamp")

    val df3 = df2.withColumn("value",from_json(col("value"),schema))
    val df4 = df3.selectExpr("key","value.*","timestamp")
    val df5 = df4.withColumn("etl_date",date_format(col("timestamp"),"yyyy-MM-dd"))
    val df6 = df5.select(col("etl_date"),concat_ws(sep=" || ",df4.col("key"),df4.col("zactcode"),df4.col("billfreq"),df4.col("timestamp")).as("data"))

    df6.writeStream.outputMode("append").partitionBy("etl_date").format("text").option("checkpointLocation", "hdfs://cnszqldlkap02.alico.corp:8020/tmp/ckp_pt").option("path", "hdfs://cnszqldlkap02.alico.corp:8020/warehouse/tablespace/external/hive/rt_sync_tmp.db/rdz_la_zpcmpf_test_pt").trigger(Trigger.ProcessingTime("2 seconds")).start().awaitTermination()

//    val query = df4.writeStream
//      .outputMode("append")
//      .format("orc")
//      .option("checkpointLocation","hdfs://cnszqldlkap02.alico.corp:8020/tmp/checkpoint")
//      .option("path","hdfs://cnszqldlkap02.alico.corp:8020/warehouse/tablespace/external/hive/rt_sync_tmp.db/rdz_la_zpcmpf_test")
//      .trigger(Trigger.ProcessingTime("2 seconds"))
//      .start()
//      .awaitTermination()

    spark.stop()
  }

}
