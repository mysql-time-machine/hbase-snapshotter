 package com.booking.spark

import com.booking.sql.{DataTypeParser, MySQLDataType}

import java.util.NavigableMap
import scala.collection.JavaConversions._

import com.google.gson.{JsonObject, JsonParser}
import com.cloudera.spark.hbase.HBaseContext
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.{Result, Scan, Get, HTable}
import org.apache.hadoop.hbase.filter.{FilterList, FirstKeyOnlyFilter, KeyOnlyFilter}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SaveMode}
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.types.
  {
    StructField,
    StructType,
    DataType,
    DoubleType,
    IntegerType,
    LongType,
    StringType
  }

/**
  * Spark application that takes a snapshot of an HBase table at a
  * given point in time and stores it to a Hive table.
  */
object HBaseSnapshotter {
  private var _hc: HiveContext = null
  private var _hbc: HBaseContext = null
  private var _args: Arguments = null
  private var _config: ConfigParser = null


  /* Readable type structure returned by the hbase client */
  private type FamilyName = Array[Byte]
  private type ColumnName = Array[Byte]
  private type Timestamp = java.lang.Long
  private type Value = Array[Byte]
  private type FamilyMap = NavigableMap[FamilyName, NavigableMap[ColumnName, NavigableMap[Timestamp, Value]]]


  /** Initialize HiveContext, HBaseContext, arguments, and configuration options
    * @param args command line arguments
    * @param config configuration file options
    */
  def init(args: Arguments, config: ConfigParser): Unit = {
    val conf = new SparkConf()
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    conf.set("spark.kryoserializer.buffer", "64k")
    conf.registerKryoClasses(Array(classOf[Result]))
    conf.setAppName("HBaseSnapshotter")

    val sc = new SparkContext(conf)
    val hbaseConfig = HBaseConfiguration.create()
    hbaseConfig.set("hbase.zookeeper.quorum", config.getZooKeeperQuorum())

    _hbc = new HBaseContext(sc, hbaseConfig)
    _hc = new HiveContext(sc)
    _args = args
    _config = config
  }


  /**
    * Transforms the data in a hashmap into a Row object.
    * The data of the current HBase row is stored in a hash map. To store them into Hive,
    * we need to feed them to an object of type Row. The elements should be in the same order
    * as the columns are written in the schema.
    *
    * @param familyMap A hashmap holding the values of the current row.
    * @param schema a struct that specifies how the schema would look like in Hive table.
    * @return an object of type Row holding the row data.
    */
  def transformMapToRow(familyMap: FamilyMap, schema: StructType): Row = {
    Row.fromSeq(for (field: StructField <- schema.fields) yield {
      try {
        val fieldValue: String = Bytes.toStringBinary(familyMap
          .get(Bytes.toBytes("d"))
          .get(Bytes.toBytes(field.name))
          .lastEntry().getValue)

        field.dataType match {
          case IntegerType => fieldValue.toInt
          case LongType => fieldValue.toLong
          case DoubleType => fieldValue.toDouble
          case _ => fieldValue
        }
      }
      catch {
        case e: Exception => null
      }
    })
  }

  def main(cmdArgs: Array[String]): Unit = {
    val args = ArgumentParser(cmdArgs)
    val config = new ConfigParser(args.configPath)

    init(args, config)


    val schema: StructType = HBaseSchema(config.getSchema)
    // val schema: StructType = MySQLSchema(_hbc, args.mySQLTableName, args.schemaTableName, args.timestamp)

    val scan = new Scan()
    if (args.timestamp > -1) scan.setTimeRange(0, args.timestamp)
    val hbaseRDD = _hbc.hbaseRDD(args.hbaseTableName, scan, { r: (ImmutableBytesWritable, Result) => r._2 })

    val rowRDD = hbaseRDD.map({ r => transformMapToRow(r.getMap(), schema) })
    val dataFrame = _hc.createDataFrame(rowRDD, schema)
    dataFrame.write.mode(SaveMode.Overwrite).saveAsTable(args.hiveTableName)
  }
}
