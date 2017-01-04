package com.example.helpers

import java.sql.ResultSet

/**
 * @author anandrajj
 * Description: API to unload a give Redshift table to S3.
 */

object RedshiftUnload extends JDBCQuery[Unit] {
  
  override def processResultSet(x: ResultSet)  = {}
  
  def unloadToS3(schema_name: String, table_name: String, s3Path:String, username: String, password: String,
      aws_access_key_id: String, aws_secret_access_key:String, url: String) {

    try {
      val q = s"""
        unload('select * from $schema_name.$table_name a
        ') to '$s3Path' credentials 'aws_access_key_id=$aws_access_key_id;aws_secret_access_key=$aws_secret_access_key'
        allowoverwrite
        gzip
    """
      executeQuery(q, username, password, url)
    } catch {
      case e: Throwable => {
        if (e.getCause.isInstanceOf[Throwable])
          e.getCause match {
            case e: ClassCastException => println("class cast exception Ignored")
            case e: Exception          => throw e
          }
        else throw e
      }
    }
  }
}