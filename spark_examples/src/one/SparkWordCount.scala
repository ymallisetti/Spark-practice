package one

import util.SparkUtil

object SparkWordCount {
   def main(args: Array[String]): Unit = {
    println("Start of SparkWordCount..")
    
    val spark = SparkUtil.getLocalSparkSession
    
    val linesRdd = spark.sparkContext.textFile("D:\\upload\\spark_data\\word_count.txt")
    linesRdd.foreach(println)
    
    val value=linesRdd.flatMap(_.split(" "))
    val pair=value.map(word=>(word,1))
    val counts=pair.reduceByKey(_+_)
    counts.foreach(println)
    println("End of SparkWordCount..")
   }
   
   
   
}