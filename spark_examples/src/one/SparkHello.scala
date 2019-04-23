package one

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SaveMode

object SparkHello {
  def main(args: Array[String]): Unit = {
    println("Start of SparkHello..")

    import org.apache.log4j.{ Level, Logger }
    Logger.getLogger("org").setLevel(Level.ERROR)

    System.setProperty("hadoop.home.dir", "D:\\upload\\dharmik\\winutils")
    val spark = SparkSession.builder().master("local[1]").getOrCreate()

    val df = spark.read
      .format("com.databricks.spark.csv")
      .option("header", "true") // Use first line of all files as header
      .option("inferSchema", "true") // Automatically infer data types
      .load("inputs/products.csv")

    //df.show(false)

    //small demo on spark word count problem
    wordCount(spark)

    println("End of SparkHello..")
  }

  def wordCount(spark: SparkSession) = {
    println("Start of word count example..")

    import spark.implicits._

    import org.apache.spark.sql.functions._
    //Counting words the DataFrame way
    val linesDF = spark.read.text("D:\\upload\\spark_data\\word_count.txt")
    val interDF=linesDF
                    .withColumn("wordsArray",split(col("value"), " "))
                    .withColumn("word", explode($"wordsArray"))
    val resultDF=interDF.groupBy("word").count
    resultDF.show
    
    //Counting words RDD way
    val linesRdd = spark.sparkContext.textFile("D:\\upload\\spark_data\\word_count.txt")
    //splitting by the space
    val wordsMap = linesRdd.map(line => line.split(" "))         //RDD[Array[String]]
    val wordsFlatMap = linesRdd.flatMap(line => line.split(" ")) //RDD[String]
    val grouped_words = wordsFlatMap.map(word => (word.toLowerCase, 1)) //(word,1)
    val counts = grouped_words.reduceByKey(_ + _)
    counts.foreach(println)

    println("End of word count example..")
  }

}