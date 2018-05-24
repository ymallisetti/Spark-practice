package com.dk.training

import org.apache.spark.SparkConf

object HelloWorld {
  def main(args:Array[String]) :Unit = {
    println("Hello world spark")
    val sc = new SparkConf().setAppName("DK hello world")
    
    println("end of hello world application..")
  }
}