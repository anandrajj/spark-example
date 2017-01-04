package com.example.helpers

import java.sql.ResultSet
import java.sql.Connection
import java.sql.DriverManager
import java.sql.SQLException

/**
 * @author anandrajj
 * Description: Interface to execute queries against the redshift database. 
 * It takes the query, user, password and database url as input.
 * It supports both queries that return a result-set and also the ones that don't return a result-set like "unload database".
 *  
 */

trait JDBCQuery[T] {
  
  def processResultSet(x: ResultSet): T
  
  def executeQuery(queryString: String, username:String, password: String, url:String) =
    {
      // connect to the database named "fixedodds" on the aws.redshift
      val driver = "org.postgresql.Driver"
      //val url = "jdbc::postgresql://analytics-production.cqhuxbkape0v.ap-southeast-2.redshift.amazonaws.com:5439/fixedodds?ssl=true&sslfactory=org.postgresql.ssl.NonValidatingFactory"

      // there's probably a better way to do this
      var connection: Connection = null
      val resutSetArray = collection.mutable.ArrayBuffer[T]()
      
      try {
        // make the connection
        DriverManager.registerDriver(new org.postgresql.Driver)
        connection = DriverManager.getConnection(url, username, password)

        // create the statement, and run the select query
        val statement = connection.createStatement()
        val resultSet = statement.executeQuery(queryString)

        while (resultSet.next()) resutSetArray += processResultSet(resultSet)

      } catch {
        case e: Throwable => throw e
      } finally {
        if (connection != null) connection.close()
      }

      resutSetArray
    }

}