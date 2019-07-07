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
  
  case class Mapping(DESTINATION_COL_NAME: String, DESTINATION_DATATYPE: String,SOURCE_COL_NAME: String, SOURCE_DATATYPE: String,TRANSFORM_TYPE: String, SOURCE_EXPRESSION: String)
  case class PPat(PAT_ID: String, FIRST_NAME: String, LAST_NAME: String, CITY: String, ZIP_CODE: String, GENDER: String, STATE: String, BIRTH_DATE:String)
  
  def readDataFrame(spark:SparkSession,location:String):DataFrame={
    val df = spark.read
      .format("com.databricks.spark.csv")
      .option("header", "true")      // Use first line of all files as header
      .option("inferSchema", "true") // Automatically infer data types
      .load(location)
    df
  }
  
  def setupLocalSparkEnv = {
    import org.apache.log4j.{ Level, Logger }
    Logger.getLogger("org").setLevel(Level.ERROR) //set the logger level 

    //set the property for hadoop home to work spark on local
    System.setProperty("hadoop.home.dir", "D:\\upload\\dharmik\\winutils")
  }
  
  def getLocalSparkSession : SparkSession = {
    setupLocalSparkEnv
    SparkSession.builder().master("local[1]").getOrCreate
  }
  
}