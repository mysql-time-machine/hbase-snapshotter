package com.booking.spark

import com.booking.sql.{DataTypeParser, MySQLDataType}
import com.cloudera.spark.hbase.HBaseContext
import org.apache.hadoop.hbase.client.{ Result, Scan }
import org.apache.hadoop.hbase.filter.{ FilterList, FirstKeyOnlyFilter, KeyOnlyFilter }
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.sql.types.{ DataType, DoubleType, IntegerType, LongType, StringType, StructField, StructType }
import com.google.gson.JsonParser

import scala.collection.JavaConversions._


object HBaseSchema {

  private def hBaseToSparkSQL(dt: String): DataType = {
    DataType.fromJson("\"" + dt + "\"")
  }

  private def transformSchema(fields: List[String]): StructType = {
    StructType(fields.map( field =>
      field.split(':') match {
        case Array(family, qualifier, dt) => StructField(qualifier, hBaseToSparkSQL(dt), true)
      }
    ))
  }

  def apply(fields: List[String]): StructType = {
    transformSchema(fields)
  }
}


object MySQLSchema {

  /** Convert MySQL datatype strings to Spark datatypes
    * @param MySQL datatype
    * @return Spark datatype
    */
  private def mySQLToSparkSQL(s: String): DataType = {
    val dt: MySQLDataType = DataTypeParser(s)
    dt.typename match {
      case "TINYINT" | "SMALLINT" | "MEDIUMINT" | "INT" | "INTEGER" => IntegerType
      case "BIGINT" => LongType
      case "NUMERIC" | "DECIMAL" | "FLOAT" | "DOUBLE" | "REAL" => DoubleType
      case _ => StringType
    }
  }


  /** Parse schema information from MySQL to HBase dump
    * @param table original MySQL table name
    * @param value json object with following structure:
    *     {table => { "columnIndexToNameMap": { index: name, ... },
    *                 "columnsSchema": { name: { "columnType": "sometype", ... }}}}
    * @return Spark schema
    */
  private def transformSchema(table: String, value: String): StructType = {
    val schemaObject = new JsonParser().parse(value)
    val tableSchema = schemaObject.getAsJsonObject().getAsJsonObject(table)
    val columnIndexToNameMap = tableSchema.getAsJsonObject("columnIndexToNameMap")
    val columnsSchema = tableSchema.getAsJsonObject("columnsSchema")

    val sortedSchema: Seq[(Int, String, String)] =
      columnIndexToNameMap.entrySet().toSeq.map({ idx => {
        val columnIndex = idx.getKey().toInt
        val columnName = idx.getValue().getAsString()
        val columnType = columnsSchema
          .getAsJsonObject(columnName)
          .getAsJsonPrimitive("columnType")
          .getAsString()
        (columnIndex, columnName, columnType)
      }}).sortBy(_._1)

    return StructType(sortedSchema.map({ field =>
      StructField(field._2, mySQLToSparkSQL(field._3), true)
    }))
  }


  /** Extract Spark schema from MySQL to HBase dump
    * @param tableName Original MySQL table name
    * @param schemaTableName HBase schema table
    * @param timestamp Closest epoch to desired version of the schema
    * @return Spark schema
    */
  def apply(hbc: HBaseContext, tableName: String, schemaTableName: String, timestamp: Long): StructType = {

    /* Replicator schema dumps are keyed on timestamp, except for the
     * initial snapshot, which is keyed on "initial-snapshot".
     *  We therefore define an explicit ordering that takes it into account
     */
    val keyOrdering: Ordering[Result] = Ordering.by[Result, Long]({ x: Result =>
      val key = Bytes.toStringBinary(x.getRow())
      key match {
        case "initial-snapshot" => 0
        case _ => key.toLong
      }
    })

    /* get correct schema row: top(1) returns greatest (latest) key in [0,
     * timestamp/now()].
     */
    val scan = new Scan()
    if (timestamp > -1) scan.setTimeRange(0, timestamp)
    scan.addColumn(Bytes.toBytes("d"), Bytes.toBytes("schemaPostChange"))
    scan.setFilter(new FilterList(FilterList.Operator.MUST_PASS_ALL, new FirstKeyOnlyFilter(), new KeyOnlyFilter()))

    val rdd = hbc.hbaseRDD(schemaTableName, scan, { r: (ImmutableBytesWritable, Result) => r._2 })
    val row = rdd.top(1)(keyOrdering).last.getRow()

    /* get correct schema json dump: since we want to use
     * HBaseContext.hbaseRDD, we need to use a Scan object. However,
     * we can't pass (row, row), because the latter is exclusive,
     * which means we get the empty set. Therefore, we start the scan
     * at the (inclusive) row, and grab the top(1).reverse, which
     * returns the smallest (earliest) row. */
    val fullScan = new Scan(row)
    fullScan.addColumn(Bytes.toBytes("d"), Bytes.toBytes("schemaPostChange"))
    fullScan.setFilter(new FirstKeyOnlyFilter())

    val fullRDD = hbc.hbaseRDD(schemaTableName, fullScan, { r: (ImmutableBytesWritable, Result) => r._2 })

    val value =
      Bytes.toStringBinary(
        fullRDD.top(1)(keyOrdering).reverse.last.getMap()
          .get(Bytes.toBytes("d"))
          .get(Bytes.toBytes("schemaPostChange"))
          .lastEntry()
          .getValue()
      )

    transformSchema(tableName, value)
  }
}
