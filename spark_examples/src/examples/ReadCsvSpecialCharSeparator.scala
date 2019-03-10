package examples

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import java.nio.charset.StandardCharsets

object ReadCsvSpecialCharSeparator {
  
  def main(args: Array[String]): Unit = {
    import org.apache.log4j.{ Level, Logger }
    Logger.getLogger("org").setLevel(Level.ERROR)

    System.setProperty("hadoop.home.dir", "D:\\upload\\dharmik\\winutils")
    val spark = SparkSession.builder().master("local[1]").getOrCreate()

    val df = spark.read
      .option("header", "true") // Use first line of all files as header
      .option("inferSchema", "true") // Automatically infer data types
      .option("charset", "ISO-8859-1") //specify character set
      .option("delimiter", "§") //provide custom delimiter
      .csv("inputs/special_char_separator.csv")

    df.show(false)

  }

}