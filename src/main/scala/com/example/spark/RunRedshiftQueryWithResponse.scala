package org.tabcorp.hulc.preprocessing

import com.example.helpers.JDBCQuery
import scala.io.Source
import com.example.helpers.InitialFunctions._
import java.sql.ResultSet


/**
 * @author: anandrajj
 * Description: Program to execute sql queries on Redshift that provides a result-set.
 */

object RunRedshiftQueryWithResponse extends App with JDBCQuery[String] {

  val argsMap = getArgsAsMap(args, List("queryFile", "username", "password", "columns", "schema", "url"))

  def getSelectColumnNames(): Array[String] = {
    argsMap("columns").split(",")
  }

  override def processResultSet(x: ResultSet) = {
    getSelectColumnNames.map(y => x.getString(y)).mkString("|")
  }

  val username = argsMap("username")
  val password = argsMap("password")
  val schema = argsMap("schema")

  val pattern = "^\\s*--.*".r

  val query_without_cmts = Source.fromFile(argsMap("queryFile")).getLines.filter(s => pattern.findFirstIn(s).isEmpty).mkString(" ")

  //require(query_without_cmts.contains("$$SCHEMA$$"))

  val query = query_without_cmts.replaceAll("\\$\\$SCHEMA\\$\\$", schema)

  val url = argsMap("url")

  //println(query)

  try {

    val out = executeQuery(query, username, password, url)
    out.foreach(System.err.println)

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