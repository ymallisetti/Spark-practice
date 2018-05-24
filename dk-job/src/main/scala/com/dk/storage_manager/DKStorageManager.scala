package com.dk.storage_manager

import com.typesafe.config.Config
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.DataFrame
import org.apache.spark.SparkContext
import org.apache.spark.sql.SaveMode
import org.apache.commons.lang3.text.StrSubstitutor
import scala.collection.JavaConversions.mapAsJavaMap
import org.apache.spark.sql.SparkSession

object DKStorageManager {
  def createDFfromCSV(csvFilePath: String, spark: SparkSession, filterCondition: String): DataFrame = {
    //val valueMap = Map("folder_name" -> configTableName, "table_name" -> configTableName)
    //val hdfsUrl = "/dharmik/config/${table_name}"
    //val hdfsUrl = "/dharmik/dkjob/config/dk_rules.csv"
    println("creating data frames from file --> "+csvFilePath)

    val CSVFormat = "com.databricks.spark.csv"
    
    val df = spark.read
      .format(CSVFormat)
      .option("header", "true")
      .option("delimiter", "|")
      .option("charset", "UTF8")
      .option("nullValue", "")
      .option("treatEmptyValuesAsNulls", "true")
      .option("inferSchema", "false")
      .load(csvFilePath)
      
    //df.show

    if (filterCondition != null && filterCondition.length() > 0)
      df.filter(filterCondition)
    else
      df
  }
}