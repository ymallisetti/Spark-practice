package com.dvk.practice.pl
import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.log4j._
object Nypd {;import org.scalaide.worksheet.runtime.library.WorksheetSupport._; def main(args: Array[String])=$execute{;$skip(183); 
  println("This is NYPD crime analysis worksheet");$skip(95); 
  
  // Set the log level to only print errors
  Logger.getLogger("org").setLevel(Level.ERROR);$skip(129); 
        
  // Create a SparkContext using every core of the local machine
  val sc = new SparkContext("local[*]", "NYPD crimes");System.out.println("""sc  : org.apache.spark.SparkContext = """ + $show(sc ))}
  
  //val crimes_rdd_raw=sc.textFile("file:///D:/Dharmik/workspace_Spark/files/NYPD_small.csv")
}
