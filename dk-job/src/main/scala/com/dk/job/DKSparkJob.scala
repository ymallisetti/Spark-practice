package com.dk.job

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SQLContext

import com.dk.job.builder.DKDataFrameBuilder
import com.dk.dq.DKDataQuality
import com.dk.util.DKUdfUtility
import com.dk.transformation.DKTransformers
import org.apache.spark.sql.{SaveMode,SparkSession}

object DKSparkJob {

  def main(args: Array[String]): Unit = {
    println(" ")
    println("********************** Start of my spark job **********************")

    val argsMap = args.map(arg => arg.split("=")).map(arr => (arr(0), arr(1))).toMap
   
    /**
     * Using SparkSession instead of SparkConf and SqlContext (2.0)
     */
    val spark = SparkSession
       .builder()
       .appName("DK spark job")
       .config("spark.driver.memory", "2G")
       .enableHiveSupport()
       .getOrCreate()
       
    spark.conf.set("spark.kyro.registrationRequired", "true")
    spark.conf.set("spark.driver.maxResultSize", "1G")
    //set arguments in spark configuration
    for((key,value)<-argsMap) spark.conf.set(key, value)

    //register UDFs
    DKUdfUtility.registerAllUdfs(spark)
    
    val inputFilesRDD = spark.sparkContext.wholeTextFiles(argsMap.getOrElse("inputFolder", None).toString())

    //------------------------creating parser data frames------------------------------------------
    val parsedDataFramesMap = DKDataFrameBuilder.createDataFrameForAllSections(spark, inputFilesRDD)

    //------------------------applying DQ rules ---------------------------------------------------
    var exceptionDataFrame: Option[DataFrame] = None
    if (parsedDataFramesMap.size > 0) {
      exceptionDataFrame = DKDataQuality.applyDQRules(spark, parsedDataFramesMap)
    }
    
    //------------------------Perform Transformation-----------------------------------------------
    DKTransformers.performTransformation(parsedDataFramesMap, spark)

    //------------------------Saving Job History --------------------------------------------------
    DKDataFrameBuilder.createAndSaveDkJobHistoryDataframe(spark, argsMap)

    println("********************* End of my spark job *************************")
    println(" ")
  }
  
  def invokeSparkJobOlderVersion(){
    val sparkConf = new SparkConf()
      .set("spark.kyro.registrationRequired", "true")
      .set("spark.driver.maxResultSize", "1G")
      .setAppName("DK spark job")
      
    val sparkContext = new SparkContext(sparkConf)
    val sqlContext = new SQLContext(sparkContext)
    
    
  }

}  

