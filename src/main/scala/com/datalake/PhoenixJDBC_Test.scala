package com.datalake

import java.sql.{Connection, DriverManager, ResultSet}

object PhoenixJDBC_Test {
  def main(args: Array[String]): Unit = {
    val jdbc_url:String = "jdbc:phoenix:cnszqldlkap01.alico.corp,cnszqldlkap02.alico.corp:2181:/hbase-secure:zookeeper/cnszqldlkap01.alico.corp@QAALICO.CORP:/etc/security/keytabs/zk.service.keytab"
    val connection :Connection = DriverManager.getConnection(jdbc_url)

    val thin_jdbc :String = "jdbc:phoenix:thin:url=https://cnszqldlkap01.alico.corp:16020"
    val t_connection :Connection = DriverManager.getConnection(thin_jdbc)


    val statement = connection.createStatement()
//    val driver = classof[org.apache.phoenix.jdbc.PhoenixDriver]
    val pst = connection.prepareStatement("select * from LDZ.LDZ_LA_ZPCMPF_TEST")
    val result:ResultSet = pst.executeQuery()

    println(result.getArray(1))

    connection.close()
  }

}
