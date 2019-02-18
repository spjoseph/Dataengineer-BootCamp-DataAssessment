name := "hbaseloadapp"
version := "1.0"
scalaVersion := "2.11.8"
libraryDependencies ++= Seq(
"org.apache.spark" % "spark-core_2.11" % "2.2.0",
"org.apache.spark" % "spark-sql_2.11" % "2.2.0",
"org.apache.spark" % "spark-streaming_2.11" % "2.2.0",
"org.apache.spark" % "spark-mllib_2.11" % "2.2.0",
"org.jmockit" % "jmockit" % "1.34" % "test",
"org.apache.hbase" % "hbase-client" % "1.1.1-mapr-1602-m7-5.2.0",
"org.apache.hbase" % "hbase-common" % "1.1.1-mapr-1602-m7-5.2.0",
"org.apache.hbase" % "hbase-server" % "1.1.1-mapr-1602-m7-5.2.0" excludeAll ExclusionRule(organization = "org.mortbay.jetty")

)