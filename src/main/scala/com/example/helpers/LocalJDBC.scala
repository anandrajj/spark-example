package com.example.helpers

import java.sql.DriverManager
import java.sql.Connection
import java.sql.ResultSet
import scala.util.Try
import scala.util.Success
import scala.util.Failure

/**
 * @author anandrajj
 * Description: Program to execute query against the local host postgres db.
 */

object LocalJDBC {

  def executeQuery(queryString: String) =
    {
      // connect to the database named "fixedodds" on the aws.redshift
      val driver = "org.postgresql.Driver"
      val url = "jdbc:postgresql://localhost:5432/test_table"
      val username = "anandraj"
      val password = "anan123"

      // there's probably a better way to do this
      //var connection: Connection = null
      val resutSetArray = collection.mutable.ArrayBuffer[(Int, String)]()
     val connection = {
        // make the connection
        val connection:Connection = Try(DriverManager.getConnection(url, username, password)) match {
          case Success(c) => c
          case Failure(e) => throw e
        }  

        // create the statement, and run the select query
        val statement = Try(connection.createStatement()) match {
          case Success(c) => c
          case Failure(e) => {connection.close(); throw e}
        }
        
        val resultSet = Try(statement.executeQuery(queryString)) match {
          case Success(c) => c
          case Failure(e) => {connection.close(); throw e}
        }

        while (resultSet.next()) { val x = (resultSet.getInt("id"), resultSet.getString("value"))
            resutSetArray += x  
        }
        
        connection

      } 
      
      connection.close()
      
      resutSetArray
    }

}