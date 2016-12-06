package com.booking.spark

import java.io.File
import com.typesafe.config.{ConfigFactory, Config, ConfigRenderOptions}
import scala.collection.JavaConversions._

object Settings {

  private val config: Config = ConfigFactory.parseFile(new File("./application.json"))
    .getConfig("HBaseSnapshotter")

  config.checkValid(ConfigFactory.defaultReference()
    .getConfig("HBaseSnapshotter"))

  val mysqlTable: String = config.getString("mysql.table")
  val mysqlSchema: String = config.getString("mysql.schema")
  val hbaseTimestamp: Long = config.getLong("hbase.timestamp")
  val hbaseZookeperQuorum: String = config.getStringList("hbase.zookeeper_quorum").toList.mkString(",")
  val hbaseSchema: List[String]  = config.getStringList("hbase.schema").toList
  val hbaseTable: String = config.getString("hbase.table")
  val hiveTable: String = config.getString("hive.table")

  println(config.root().render(ConfigRenderOptions.concise().setFormatted(true)))
}
