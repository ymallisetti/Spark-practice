package util

/**
 * all the imports may not be used in the class but it is for the reference for other classes
 */
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Encoders


object SparkUtil {
  
  def readDataFrame(spark:SparkSession,location:String):DataFrame={
    val df = spark.read
      .format("com.databricks.spark.csv")
      .option("header", "true")      // Use first line of all files as header
      .option("inferSchema", "true") // Automatically infer data types
      .load(location)
    df
  }
  
}