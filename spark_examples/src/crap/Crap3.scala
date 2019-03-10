package crap

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SaveMode
import java.nio.charset.StandardCharsets

object Crap3 {
  def main(args: Array[String]): Unit = {
    import org.apache.log4j.{ Level, Logger }
    Logger.getLogger("org").setLevel(Level.ERROR)

    System.setProperty("hadoop.home.dir", "D:\\upload\\dharmik\\winutils")
    val spark = SparkSession.builder().master("local[1]").getOrCreate()

    val df = spark.read
      .format("com.databricks.spark.csv")
      .option("header", "true") // Use first line of all files as header
      .option("inferSchema", "true") // Automatically infer data types
      .option("charset", "utf-16")
      .option("delimiter", "§") //provide custom delimiter
      .load("inputs/Finalcsv.csv")

    df.show(false)

  }
}