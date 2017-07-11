package com.booking.spark

import java.io.File
import com.typesafe.config.{ConfigFactory, Config, ConfigRenderOptions, ConfigException}
import scala.collection.JavaConversions._
import org.apache.log4j.{Level, Logger}

class Settings() {
  private val logger = Logger.getLogger(this.getClass())
  logger.setLevel(Level.DEBUG)

  private val config: Config = ConfigFactory.load()

  private def formatConfig(config: Config): String = config.root().render(ConfigRenderOptions.concise().setFormatted(true))

  def getSchema(): SnapshotterSchema = {
    try {
      config.checkValid(ConfigFactory.defaultReference().getConfig("HBaseSnapshotter.MySQLSchema"))
      logger.info(s"Detected valid $MySQLSchema")
      MySQLSchema
    } catch {
      case e: ConfigException => {
        try
        {
          config.checkValid(ConfigFactory.defaultReference().getConfig("HBaseSnapshotter.HBaseSchema"))
          logger.info(s"Detected valid $HBaseSchema")
          HBaseSchema
        }
        catch {
          case e: ConfigException => {
            logger.error(
              s"""Bad config. Your application.json:
                  |${formatConfig(config)}
                  |HBaseSchema format:
                  |${formatConfig(ConfigFactory.defaultReference().getConfig("HBaseSnapshotter.HBaseSchema"))}
                  |MySQLSchema format:
                  |${formatConfig(ConfigFactory.defaultReference().getConfig("HBaseSnapshotter.MySQLSchema"))}
                  |$e
              """.stripMargin)
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
}
