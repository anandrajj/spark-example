package org.tabcorp.hulc.preprocessing

import com.example.helpers.JDBCQuery
import java.sql.ResultSet
import scala.io.Source
import com.example.helpers.InitialFunctions._

/**
 * @author: anandrajj
 * Description: Scala program to execute query on Redshift that doesn't provide a response like unload or update.
 */
object RunRedshiftQueries extends App with JDBCQuery[Unit] {

  override def processResultSet(x: ResultSet) = {}

  val argsMap = getArgsAsMap(args, List("queryFile", "username", "password", "schema", "url"))

  val username = argsMap("username")
  val password = argsMap("password")
  val schema = argsMap("schema")
  val url = argsMap("url")

  val aws_acesss_key = sys.env("AWS_ACCESS_KEY_ID")
  val aws_secret_access_key = sys.env("AWS_SECRET_ACCESS_KEY")

  val pattern = "^\\s*--.*".r

  val query_cmts_removed = Source.fromFile(argsMap("queryFile")).getLines.filter(s => pattern.findFirstIn(s).isEmpty).mkString(" ")

  //require(query_cmts_removed.contains("$$SCHEMA$$"))
   // && query_cmts_removed.contains("AWSACCESSKEY")
   // && query_cmts_removed.contains("AWSSECRETACCESSKEY"))

  val query = query_cmts_removed.replaceAll("\\$\\$SCHEMA\\$\\$", schema)
    .replaceAll("AWSACCESSKEY", aws_acesss_key)
    .replaceAll("AWSSECRETACCESSKEY", aws_secret_access_key)

  

  //println(query)

  try {

    executeQuery(query, username, password, url)

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