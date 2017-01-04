package com.example.helpers

import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.StructType

/**
 * @author: anandrajj
 * Description: API to readCSV with specified schema into Spark as DataFrame.
 */

object ReadCSVIntoDataFrame {
  
  def readCSVIntoDataFrame(s3File: String, tableSchema: StructType, fieldSep:String)
  (implicit sqlContext: SQLContext): DataFrame = {
    
    sqlContext.read
      .format("com.databricks.spark.csv")
      .option("header", "false")
      .option("delimiter", fieldSep)
      .schema(tableSchema)
      .load(s3File)
  }
  
  
}