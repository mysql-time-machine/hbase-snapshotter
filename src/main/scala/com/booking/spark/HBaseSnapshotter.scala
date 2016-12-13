package com.booking.spark

import com.booking.sql.{DataTypeParser, MySQLDataType}
import java.io.File

import java.util.NavigableMap

import com.google.gson.{JsonObject, JsonParser}
import org.apache.hadoop.hbase.spark.HBaseContext
import org.apache.hadoop.hbase.{TableName, HBaseConfiguration}
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
  /* Readable type structure returned by the hbase client */
  private type FamilyName = Array[Byte]
  private type ColumnName = Array[Byte]
  private type Timestamp = java.lang.Long
  private type Value = Array[Byte]
  private type FamilyMap = NavigableMap[FamilyName, NavigableMap[ColumnName, NavigableMap[Timestamp, Value]]]

  /**
    * Transforms the data in a hashmap into a Row object.
    * The data of the current HBase row is stored in a hash map. To store them into Hive,
    * we need to feed them to an object of type Row. The elements should be in the same order
    * as the columns are written in the schema.
    *
    * @param result a Result holding the values of the current row.
    * @param schema a StructType that specifies how the schema would look like in Hive table.
    * @param keepKey a Boolean that indicates whether we should keep the HBase row key value.
    * @return an object of type Row holding the row data.
    */
  def transformMapToRow(result: Result, schema: StructType, keepKey: Boolean): Row = {
    val familyMap: FamilyMap = result.getMap()
    val fields = for (field: StructField <- schema.fields) yield {
      if (keepKey && field.metadata.contains("key"))
        Bytes.toStringBinary(result.getRow())
      else
        try {
          val fieldValue: String = Bytes.toStringBinary(familyMap
            .get(Bytes.toBytes(field.metadata.getString("family")))
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
          case e: Exception => {
            println(e)
            null
          }
        }
    }
    Row.fromSeq(fields)
  }

  def main(cmdArgs: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    conf.set("spark.kryoserializer.buffer", "64k")
    conf.registerKryoClasses(Array(classOf[Result]))
    conf.setAppName("HBaseSnapshotter")

    val hbaseConfig = HBaseConfiguration.create()
    hbaseConfig.set("hbase.zookeeper.quorum", Settings.hbaseZookeperQuorum)

    val sc = new SparkContext(conf)
    val hc: HiveContext = new HiveContext(sc)
    val hbc: HBaseContext = new HBaseContext(sc, hbaseConfig)

    // val schema: StructType = HBaseSchema(Settings.hbaseSchema)
    val schema: StructType = MySQLSchema(hbc, Settings.mysqlTable, Settings.mysqlSchema, Settings.hbaseTimestamp)

    val scan = new Scan()
    if (Settings.hbaseTimestamp > -1) scan.setTimeRange(0, Settings.hbaseTimestamp)

    val hbaseRDD = hbc.hbaseRDD(TableName.valueOf(Settings.hbaseTable), scan, { r: (ImmutableBytesWritable, Result) => r._2 })

    val rowRDD = hbaseRDD.map({ r => transformMapToRow(r, schema, true) })
    val dataFrame = hc.createDataFrame(rowRDD, schema)
    dataFrame.show(10)
    dataFrame.write.mode(SaveMode.Overwrite).saveAsTable(Settings.hiveTable)
  }
}
