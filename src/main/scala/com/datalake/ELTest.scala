package com.datalake

import org.apache.spark.sql.SparkSession

object ELTest {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
        .builder()
        .appName("EL Test")
        .getOrCreate()

    println("spark is running")

    spark.stop()
  }

}
