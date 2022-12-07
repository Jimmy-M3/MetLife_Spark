package com.datalake

import org.apache.spark.sql.{SaveMode, SparkSession}

import java.sql.{Connection, DriverManager}



object PhoniexTest {
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


    val df1 = spark.read.format("jdbc").option("url","jdbc:phoenix:cnszqldlkap01.alico.corp,cnszqldlkap02.alico.corp:2181:/hbase-secure").option("driver","org.apache.phoenix.jdbc.PhoenixDriver").option("user","eos").option("dbtable","LDZ.LDZ_LA_ZPCMPF_TEST").load()
    df1.show()
    val df2 = spark.sqlContext.read.format("jdbc").option("driver","org.apache.phoenix.jdbc.PhoenixDriver").option("url","jdbc:phoenix:cnszqldlkap01.alico.corp:2181:/hbase-secure").option("dbtable","LDZ.LDZ_LA_ZPCMPF_TEST").load()

    val df = spark.sqlContext.read.format("phoenix").options(Map("table" -> "LDZ.LDZ_LA_ZPCMPF_TEST", PhoenixDataSource.ZOOKEEPER_URL -> "cnszqldlkap01.alico.corp,cnszqldlkap02.alico.corp:2181")).load()

    val conn:Connection = DriverManager.getConnection("cnszqldlkap01.alico.corp,cnszqldlkap02.alico.corp:2181:/hbase-secure")
    val stat = conn.createStatement()

    //    df.write
//        .format("jdbc")
//        .option()
//
//    df.write
//      .format("phoenix")
//      .mode(SaveMode.Overwrite)
//      .options(Map("table" -> "OUTPUT_TABLE", PhoenixDataSource.ZOOKEEPER_URL -> "phoenix-server:2181"))
//      .save()

    spark.stop()
  }
}
