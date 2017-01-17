package com.booking.spark

import scala.collection.JavaConversions._

import com.booking.sql.{DataTypeParser, MySQLDataType}
import org.apache.hadoop.hbase.spark.HBaseContext
import com.google.gson.{GsonBuilder, JsonParser, JsonObject, Gson}
import org.apache.log4j.{Level, Logger}
import org.apache.hadoop.hbase.{TableName}
import org.apache.hadoop.hbase.client.{Result, Scan}
import org.apache.hadoop.hbase.filter.{FilterList, FirstKeyOnlyFilter, KeyOnlyFilter}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.spark.HBaseContext
import org.apache.spark.sql.types.{
  DataType,
  DoubleType,
  IntegerType,
  LongType,
  Metadata,
  StringType,
  StructField,
  StructType
}

abstract class SnapshotterSchema {
  protected val logger = Logger.getLogger(this.getClass())
  logger.setLevel(Level.DEBUG)
  def apply(hbc: HBaseContext, settings: Settings): StructType
}

object HBaseSchema extends SnapshotterSchema {
  private val gson = new GsonBuilder().create()

  private def hBaseToSparkSQL(dt: String): DataType = DataType.fromJson(gson.toJson(dt))

  private def transformSchema(fields: List[String]): StructType = {
    StructType(fields.map( field =>
      field.split(':') match {
        case Array(family, qualifier, dt) => {
          val datatype = hBaseToSparkSQL(dt)
          logger.info(s"Datatype for $qualifier present in schema ($datatype)")
          val metadata: Metadata = Metadata.fromJson(Utils.toJson(Map("family" -> family)))
          StructField(qualifier, hBaseToSparkSQL(dt), true, metadata)
        }
        case Array(family, qualifier) => {
          logger.warn(s"Datatype for $qualifier is missing from schema. Defaulting to $StringType")
          val metadata: Metadata = Metadata.fromJson(Utils.toJson(Map("family" -> family)))
          StructField(qualifier, StringType, true, metadata)
        }
      }
    ))
  }

  def apply(hbc: HBaseContext, settings: Settings): StructType = {
    val fields: List[String] = settings.hbaseSchema()
    transformSchema(fields)
  }
}


object MySQLSchema extends SnapshotterSchema {
  private val gson = new GsonBuilder().create()

  /** Convert MySQL datatype strings to Spark datatypes
    * @param MySQL datatype
    * @return Spark datatype
    */
  private def mySQLToSparkSQL(s: String): DataType = {
    val dt: MySQLDataType = DataTypeParser(s)
    val matchedType = dt.typename match {
      case "TINYINT" | "SMALLINT" | "MEDIUMINT" | "INT" | "INTEGER" =>
        if (dt.qualifiers.contains("UNSIGNED")) {
          LongType
        }
        else {
          IntegerType
        }
      case "BIGINT" | "NUMERIC" | "DECIMAL" | "FLOAT" | "DOUBLE" | "REAL" => DoubleType
      case _ => StringType
    }
    logger.info(s"Parsing $s as $matchedType")
    matchedType
  }


  /** Parse schema information from MySQL to HBase dump
    * @param value json object with following structure:
    *     {table => { "columnIndexToNameMap": { index: name, ... },
    *                 "columnsSchema": { name: { "columnType": "sometype", ... }}}}
    * @return Spark schema
    */
  private def transformSchema(value: String, settings: Settings): StructType = {
    val tableName = settings.mysqlTable().split("\\.") match {
      case Array(_db, _table) => _table
      case Array(_table) => _table
    }
    val schemaObject = new JsonParser().parse(value)
    val tableSchema = schemaObject.getAsJsonObject().getAsJsonObject(tableName)
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

    /* "hbase_row_key" will be the first column in hive land.  It is meant
     *  to be used for deduplicating rows in delta imports containing
     *  row updates.  (group by hbase_row_key and select the latest)
     */
    val hbaseRowKey = StructField(
      "hbase_row_key",
      StringType,
      false,
      Metadata.fromJson(Utils.toJson(Map("key" -> "true")))
    )

    /* "row_status" is a fake column inserted by the replicator.  It denotes
     * whether the row is the result of a schema change (deletion,
     * update, etc)
     */
    val hbaseRowStatus = StructField(
      "row_status",
      StringType,
      false,
      Metadata.fromJson(Utils.toJson(Map("status" -> "true", "family" -> "d"))))

    return StructType(
      hbaseRowKey +: hbaseRowStatus +:
        sortedSchema.map({ field =>
          val metadata: Metadata = Metadata.fromJson(Utils.toJson(Map("family" -> "d")))
          StructField(field._2, mySQLToSparkSQL(field._3), true, metadata)
        }))
  }


  /** Extract Spark schema from MySQL to HBase dump
    * @param hbc hbase context
    * @return Spark schema
    */
  def apply(hbc: HBaseContext, settings: Settings): StructType = {
    val schemaTableName = settings.mysqlSchema()
    val timestamp = settings.hbaseTimestamp()
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

    val rdd = hbc.hbaseRDD(TableName.valueOf(schemaTableName), scan, { r: (ImmutableBytesWritable, Result) => r._2 })
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

    val fullRDD = hbc.hbaseRDD(TableName.valueOf(schemaTableName), fullScan, { r: (ImmutableBytesWritable, Result) => r._2 })

    val value =
      Bytes.toStringBinary(
        fullRDD.top(1)(keyOrdering).reverse.last.getMap()
          .get(Bytes.toBytes("d"))
          .get(Bytes.toBytes("schemaPostChange"))
          .lastEntry()
          .getValue()
      )

    transformSchema(value, settings)
  }
}
