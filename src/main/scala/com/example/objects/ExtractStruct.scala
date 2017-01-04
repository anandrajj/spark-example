package com.example.objects

import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.SQLContext
import com.example.helpers.HelperFunctions
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.types.TimestampType
import org.apache.spark.sql.types.DateType
import org.apache.spark.sql.types.DateType
import org.apache.spark.sql.types.StructField
import com.example.helpers.ReadCSVIntoDataFrame
import java.sql.DriverManager
import java.sql.Connection
import org.apache.spark.sql.Row
import java.sql.ResultSet
import org.apache.spark.sql.types.StructField
import com.example.helpers.JDBCQuery
import scala.collection.mutable.ArrayBuffer
import org.apache.spark.sql.types.LongType

/**
 * @author: anandrajj
 * Description: API to get schema from Redshift table or from file and return in StructType format.
 */

object ExtractStruct extends JDBCQuery[StructField] {

  val csvSchema = StructType(Array(
    StructField("column_name", StringType, false),
    StructField("data_type", StringType, false),
    StructField("is_nullable", StringType, false)))

  def getDataType(x: String) = {
    x match {
      case "numeric"   => DoubleType
      case "varchar"   => StringType
      case "integer"   => IntegerType
      case "smallint"  => IntegerType
      case "timestamp" => StringType
      case "date"      => StringType
      case "bigint"    => LongType
    }
  }

  def getNullable(x: String) = {
    x match {
      case "true"  => true
      case "false" => false
    }
  }
  def processResultSet(x: ResultSet) = {
    val column_name = x.getString("column_name")
    val data_type = getDataType(x.getString("data_type"))
    val is_nullable = getNullable(x.getString("is_nullable"))

    StructField(column_name, data_type, is_nullable)
  }

  def jdbcSchemaToStruct(x: Row) = {
    val column_name = x.getAs[String]("column_name")
    val data_type = getDataType(x.getAs[String]("data_type"))
    val is_nullable = getNullable(x.getAs[String]("is_nullable"))

    StructField(column_name, data_type, is_nullable)
  }

  def fromCSV(csvFile: String, fieldSep: String = HelperFunctions.fieldSep)(implicit sqlContext: SQLContext) = {
    import sqlContext.implicits._

    StructType(ReadCSVIntoDataFrame.readCSVIntoDataFrame(csvFile, csvSchema, fieldSep)
      .map(jdbcSchemaToStruct).collect)
  }

  def fromJDBC(table_name: String, schema_name: String, username: String, password: String, url: String) = {

    val struct = ArrayBuffer[StructField]()

    try {

      val q = s"""
          select ordinal_position, 
            column_name, 
            CASE WHEN data_type='character varying' THEN 'varchar'
                 WHEN data_type = 'timestamp without time zone' THEN 'timestamp'
                 WHEN data_type = 'double precision' THEN 'numeric'
                 ELSE data_type
       END,
       CASE WHEN is_nullable = 'YES' THEN 'true'
            ELSE 'false'
       END as is_nullable
       from INFORMATION_SCHEMA.COLUMNS where table_schema = '$schema_name'
       and table_name = '$table_name'
       order by ordinal_position"""

      struct ++= executeQuery(q, username, password, url)

    } catch {
      case e: Throwable => e.printStackTrace
    }

    StructType(struct)
  }
}
