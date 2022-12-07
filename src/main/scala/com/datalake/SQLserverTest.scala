package com.datalake

import java.text.SimpleDateFormat
import java.util.{Calendar, Date}
import org.apache.spark.sql.SparkSession

object SQLserverTest {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("Kafka Test")
      .getOrCreate()

    val nowDate = new Date()
    val cal1 = Calendar.getInstance()
    cal1.setTime(nowDate)
    cal1.add(Calendar.DATE, -1)

    var strDate = new SimpleDateFormat("yyyy-MM-dd").format(cal1.getTime())

    val hostname: String = "10.164.26.74"
    val port: String = "1433"
    val dbname: String = "stage"
    val user: String = "odsadmin"
    val password: String = "User123$"
    val driver:String = "com.microsoft.sqlserver.jdbc.SQLServerDriver"
    val dbtable:String = s"(SELECT [HIST_ID],[ZACTCODE],[BILLFREQ],[CRTABLE],[CNTTYPE],[ZPLANDESC],[ETL_DATATIME] FROM [Stage].[dbo].[LA_ZPCMPF] WHERE ETL_DATATIME >= $strDate) as tmp"


    val df = spark.read
      .format("jdbc")
      .option("url", s"jdbc:sqlserver://$hostname:$port")
      .option("driver", driver)
      .option("user", user)
      .option("password", password)
      .option("dbtable", dbtable)
      .load()

    df.show()

    spark.stop()
  }
}
