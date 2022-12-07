ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.11.12"

lazy val root = (project in file("."))
  .settings(
    name := "SparkTest"
  )

libraryDependencies ++= Seq(
  // https://mvnrepository.com/artifact/org.apache.spark/spark-core
  "org.apache.spark" %% "spark-core" % "2.3.2" % "provided",
  // https://mvnrepository.com/artifact/org.apache.spark/spark-sql
  "org.apache.spark" %% "spark-sql" % "2.3.2" % "provided",
    // https://mvnrepository.com/artifact/org.apache.phoenix/phoenix
  "org.apache.phoenix" % "phoenix" % "5.0.0-HBase-2.0" pomOnly(),
  "com.microsoft.sqlserver" % "mssql-jdbc" % "11.2.0.jre18",
//  "org.apache.phoenix" % "phoenix-spark" % "5.0.0-HBase-2.0"

)
