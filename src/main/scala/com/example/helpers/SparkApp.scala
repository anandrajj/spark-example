package com.example.helpers

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext

/**
 * @author: anandrajj
 * Description : provides functions such as parsing arguments, creation spark context and 
 * enforcing mandatory parms required for processing. This helps to reduce the boile plat code in spark apps.
 */

object InitialFunctions {

  val argNamePattern = "(--)([a-zA-Z0-9_]*)".r
  val mandatoryParms:List[String] = List("environment")

  def checkMandatoryCustomParms(argsMap: Map[String, String], mandatoryUserParms:List[String]) {

    mandatoryUserParms.foreach(x => if (!argsMap.contains(x))
      throw new IllegalArgumentException("Mandatory Parm %s is not found".format(x)))
  }
  
  def checkMandatoryParms(argsMap: Map[String, String], mandatoryUserParms:List[String]) {

    (mandatoryParms ++ mandatoryUserParms).foreach(x => if (!argsMap.contains(x))
      throw new IllegalArgumentException("Mandatory Parm %s is not found".format(x)))
  }

  def getArgsAsMap(args: Array[String], userMandatoryParms: List[String]) = {
    if ((args.size % 2) == 1) throw new IllegalArgumentException("Number of args must be even. But odd number of arguments found!!")

    val argsMap = args.grouped(2).map(x => {
      val y = (x(0), x(1))

      y match {
        case (argNamePattern(hyphens, argName), value) => (argName, value)
        case _ => throw new IllegalArgumentException("""argName pattern is invalid. It must start with -- followed by Alphanumeric. Eg --Parm1""")
      }
    }).toMap

    //println("Input Args found:")
    //println("-----------------")

    //argsMap.foreach(x => println("%s -> %s".format(x._1, x._2)))

    checkMandatoryParms(argsMap, userMandatoryParms)
    
    argsMap
  }

  def getSparkContext(args: Array[String], caller: String, userMandatoryParms: List[String] = List()) = {

    val argsMap = getArgsAsMap(args, userMandatoryParms)

    //Prepare spark config based on where the code is executing.
    val (conf, parts) = argsMap("environment") match {
      case "live" => (new SparkConf(), argsMap.getOrElse("parts", "3500").toInt)
      case "local" => (new SparkConf().setMaster("local[6]").set("spark.executor.memory", "5g"),
        argsMap.getOrElse("parts", "35").toInt)
      case _ => throw new IllegalArgumentException("First Argument must specify local or live")
    }

    //Set App Name & create spark context
    conf.setAppName(caller)
    val sc = new SparkContext(conf)

    val sqlContext = new SQLContext(sc)
    (sc, sqlContext, argsMap ,parts)
  }

}

trait SparkApp extends App {
  def mandatoryParms: List[String] = List()
  
  implicit lazy val (sc, sqlContext, argsMap, parts) = InitialFunctions.getSparkContext(args, this.getClass.getSimpleName, mandatoryParms)

}