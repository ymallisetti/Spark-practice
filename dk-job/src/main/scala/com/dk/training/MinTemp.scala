package com.dk.training

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import scala.math.min

object MinTemp {
  def main(args:Array[String]):Unit={
    println("********************** Min temp **********************")
    val conf = new SparkConf().setAppName("Min temperature test")
    val sc = new SparkContext(conf)
    val lines = sc.textFile("/dharmik/test/training/1800.csv")
    
    //station id, type,value
    val tuple = lines.map(line=>parseLine(line))
    
    val filteredTuple = tuple.filter(x=>x._2 == "TMIN")
    
    val pairRdd = filteredTuple.map(t=> (t._1,t._3))
    
    val mins = pairRdd.reduceByKey( (x,y) => min(x,y))
    
    mins.foreach(println)
  }
  
  def parseLine(line:String):(String,String,Int) = {
    
    val splittedLine = line.split(",")
    
    (splittedLine(0),splittedLine(2),splittedLine(3).toInt)
  }
}
