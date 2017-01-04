package com.example.spark

import org.apache.spark._
import org.apache.spark.SparkContext._
import com.example.helpers.SparkApp

object WordCount extends SparkApp {

  override def mandatoryParms = List("inputFile", "outputFile")

  import sqlContext.implicits._

  val inputFile = argsMap("inputFile")
  val outputFile = argsMap("outputFile")

  // Load our input data.
  val input = sc.textFile(inputFile)
  // Split up into words.
  val words = input.flatMap(line => line.split(" "))
  // Transform into word and count.
  val counts = words.map(word => (word, 1)).reduceByKey { case (x, y) => x + y }
  // Save the word count back out to a text file, causing evaluation.
  counts
    .saveAsTextFile(outputFile)
}