package com.example.spark

import org.apache.spark.sql.SaveMode
import com.example.helpers.{ ReadCSVIntoDataFrame, SparkApp }
import com.example.objects.ExtractStruct
import com.example.helpers.HelperFunctions
import com.example.helpers.InitialFunctions
import com.example.helpers.RedshiftUnload
import scala.collection.mutable.ListBuffer

/**
 * @author anandrajj
 * Description: Spark program to convert a CSV with specified schema into a parquet file. It can take schema
 * through a file or extract directly file Redshift table. It also can unload the Redshift table to CSV file 
 * before the conversion process.
 * 
 */

object CSVToParquet extends SparkApp {

  import sqlContext.implicits._

  override def mandatoryParms = List("tableName", "s3File", "outputParquetFile", "schemaSource")
  
  def checkMandatoryParms(argsMap:Map[String, String], schemaSource: String, unloadRedshiftTable: Boolean) = {
    
    val argList = ListBuffer[String]()
    
    schemaSource match {
      case "redshift" => {
        argList ++= List("schemaName", "username", "password", "url")
      }
      case "csv" => {
        argList ++=  List("schemaFile")
      }
      case _ => throw new IllegalArgumentException("Schema Source can only table redshift or csv. Invalid value %s".format(schemaSource))
    }
    
    if(unloadRedshiftTable) {
      argList ++= List("schemaName", "username", "password", "aws_access_key_id", "aws_secret_access_key", "url")
    }
    
    InitialFunctions.checkMandatoryCustomParms(argsMap, argList.toSet.toList)
  }
  
  val s3File = argsMap("s3File")
  val parquetFile = argsMap("outputParquetFile")
  val schemaSource = argsMap("schemaSource")
  val partsToUse = argsMap.get("parts")
  val partitionBy = argsMap.get("partitionBy")
  
  val unloadRedshiftTable = argsMap.getOrElse("unloadRedshiftTable", "false") match {
    case "true"  => true
    case "false" => false
    case x       => throw new IllegalArgumentException("Argumet --unloadRedshiftTable can only take true or false. %s is illegal".format(x))
  }

  checkMandatoryParms(argsMap, schemaSource, unloadRedshiftTable)
  
  val tableSchema = if(schemaSource == "redshift") {
      ExtractStruct.fromJDBC(argsMap("tableName"), argsMap("schemaName"),
          argsMap("username"), argsMap("password"), argsMap("url"))
  }
  
  else ExtractStruct.fromCSV(argsMap("schemaFile"))

  if(unloadRedshiftTable) {
    
    RedshiftUnload.unloadToS3(argsMap("schemaName"), argsMap("tableName"), s3File,
      argsMap("username"), argsMap("password"), argsMap("aws_access_key_id"), argsMap("aws_secret_access_key"), argsMap("url"))
      }
  
  tableSchema.fields.foreach(x => println(x.name, x.dataType))
      
  val df = ReadCSVIntoDataFrame.readCSVIntoDataFrame(s3File, tableSchema, HelperFunctions.fieldSep)
  
  if (partsToUse.isDefined && partitionBy.isDefined) throw new IllegalArgumentException("Use either parts or partitioBy. Don't use both!!!")
  
  val dfPartitioned = if(partsToUse.isDefined) df.repartition(partsToUse.get.toInt)
  else if(partitionBy.isDefined) df.repartition(partitionBy.get.split(",").map(df.col): _*)
  else df
  
  
  dfPartitioned.write
    .mode(SaveMode.Overwrite)
    .parquet(parquetFile)
}