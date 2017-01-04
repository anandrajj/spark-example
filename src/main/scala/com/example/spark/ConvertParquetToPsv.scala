package com.example.spark

import com.example.helpers.SparkApp
import com.databricks.spark.csv
import org.apache.spark.sql.SaveMode
/**
 * @author anandrajj
 * Description: Simple program to take a parquet file and convert to CSV. The program has to be executed 
 * using spark-submit command with mandatory inputFile and outputFile name arguments on top of requirements enforced by SparkApp
 */
object ConvertParquetToPsv extends SparkApp {
  
  override def mandatoryParms = List("inputFile", "outputFile")
  
  import sqlContext.implicits._
  
  sqlContext.read.parquet(argsMap("inputFile"))
  .write
  .mode(SaveMode.Overwrite)
  .format("com.databricks.spark.csv")
  .option("delimiter", argsMap.getOrElse("delimiter","|"))
  .option("codec", "gzip")
  .save(argsMap("outputFile"))
  
}