package com.booking.spark

import java.io.File
import com.typesafe.config.{ConfigFactory, Config, ConfigRenderOptions, ConfigException}
import scala.collection.JavaConversions._
import org.slf4j.{Logger, LoggerFactory}

class Settings(path: String) {
  private val logger = LoggerFactory.getLogger(this.getClass())
  private val config: Config = ConfigFactory.parseFile(new File(path))

  private def formatConfig(config: Config): String = {
    config.root().render(ConfigRenderOptions.concise().setFormatted(true))
  }

  def getSchema(): SnapshotterSchema = {
    try {
      config.checkValid(ConfigFactory.defaultReference().getConfig("HBaseSnapshotter.MySQLSchema"))
      MySQLSchema
    } catch {
      case e: ConfigException => {
        try
        {
          config.checkValid(ConfigFactory.defaultReference().getConfig("HBaseSnapshotter.HBaseSchema"))
          HBaseSchema
        }
        catch {
          case e: ConfigException => {
            logger.error(
              """Bad config. Your application.json:
                 |{}
                 |HBaseSchema format:
                 |{}
                 |MySQLSchema format:
                 |{}
                 |{}
              """.stripMargin,
              formatConfig(config),
              formatConfig(ConfigFactory.defaultReference().getConfig("HBaseSnapshotter.HBaseSchema")),
              formatConfig(ConfigFactory.defaultReference().getConfig("HBaseSnapshotter.MySQLSchema")),
              e)
            System.exit(1)
            HBaseSchema
          }
        }
      }
    }
  }

  val schemaType: SnapshotterSchema = getSchema()
  def mysqlTable(): String = config.getString("mysql.table")
  def mysqlSchema(): String = config.getString("mysql.schema")
  def hbaseTimestamp(): Long = config.getLong("hbase.timestamp")
  def hbaseZookeperQuorum(): String = config.getStringList("hbase.zookeeper_quorum").toList.mkString(",")
  def hbaseSchema(): List[String]  = config.getStringList("hbase.schema").toList
  def hbaseTable(): String = config.getString("hbase.table")
  def hiveTable(): String = config.getString("hive.table")
  def hivePartitions(): List[String] = config.getStringList("hive.partitions").toList
}
