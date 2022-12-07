package com.datalake

import org.apache.spark.sql.{SaveMode, SparkSession}

object HiveTest {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("EL Test")
      .getOrCreate()

//    Sqlserver连接信息 （同上文）
    val hostname:String = "10.164.26.74"
    val port:String = "1433"
    val dbname:String = "stage"
    val user:String = "odsadmin"
    val password:String = "User123$"

    val ods_url:String = s"jdbc:sqlserver://${hostname}:${port};databaseName=${dbname}"

    val zpcm = spark
      .read
      .format("jdbc")
      .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver")
      .option("url", ods_url)
      .option("dbtable", "LA_ZPCMPF")
      .option("user", user)
      .option("password", password)
      .load()

    zpcm.write.mode(SaveMode.Append).insertInto("rt_sync_tmp.rdz_la_zpcmpf_test")

    spark.stop()

  }
}
