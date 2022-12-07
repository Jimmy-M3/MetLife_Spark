package com.datalake

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.Trigger

object KafkaTest {
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
    import spark.implicits._
    val df2 = df.selectExpr("CAST(key AS STRING)", "CAST(topic AS STRING)", "CAST(value AS STRING)")


    val query = df2.writeStream
        .outputMode("append")
        .format("console")
        .trigger(Trigger.ProcessingTime("2 seconds"))
        .start()
        .awaitTermination()


    spark.stop()
  }
}
