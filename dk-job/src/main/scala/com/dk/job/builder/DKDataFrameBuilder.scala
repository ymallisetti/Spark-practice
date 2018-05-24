package com.dk.job.builder

import org.apache.spark.{ SparkConf, SparkContext }
import scala.collection.immutable.{ Map => iMap }
import scala.collection.mutable.Map
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.{ DataFrame, Row, SQLContext, SaveMode }
import com.dk.schema.Schema
import org.apache.spark.rdd.RDD
import com.dk.parser.DummyParser
import com.dk.beans.DkJobHistory
import com.dk.util.UtilityFunctions
import com.dk.util.Constants
import org.apache.spark.sql.DataFrame
import com.typesafe.config.{ Config, ConfigFactory }
import com.dk.storage_manager.DKStorageManager
import scala.collection.JavaConversions.mapAsJavaMap
import org.apache.commons.lang3.text.StrSubstitutor
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.{SaveMode,SparkSession}
import com.dk.job.DKParserWrapper

object DKDataFrameBuilder {

  /**
   * this function creates data frames from the parsed cda document and returns a map
   * of String and dataframes of all the parsed sections
   */
  def createDataFrameForAllSections(spark: SparkSession, inputFilesRDD: RDD[(String, String)]): iMap[String, DataFrame] = {

    val parsedDataFrameMap = Map[String, DataFrame]().empty
    //invoke the parser
    val listParserRdd :RDD[ List[(String, List[Row])] ] = inputFilesRDD.map(tuple => DKParserWrapper.parsePurchaseOrderMsgs(tuple._1, tuple._2))
    val schemaMap = UtilityFunctions.getOrCreatePOSchema(Constants.applicationConfgFileName)
    
    val flattenRdd = listParserRdd.flatMap(flattenRdd => flattenRdd).persist()

    val reduceRowRDD = flattenRdd.reduceByKey {
      case (existingRowList, newRowList) => {
        existingRowList.++(newRowList)
      }
    }

    println("Printing all entities")
    reduceRowRDD.collectAsMap().foreach {
      case (key, rowValRDD) => {
        val rddRow: RDD[Row] = spark.sparkContext.parallelize(rowValRDD)
        //add key and data to map
        println("-- "+key)
        parsedDataFrameMap += (key -> spark.createDataFrame(rddRow, schemaMap.get(key).get))
        spark.createDataFrame(rddRow, schemaMap.get(key).get).show()
      }
    }

    parsedDataFrameMap.toMap
  }

  def getDQConfigDataFrame(spark: SparkSession, properties: Config): DataFrame = {
    val rules_file_hdfs_location = Constants.RulesFileLocation
    val orgCode = spark.conf.get("orgCode")

    val filterCondition = "ORG_CODE='" + orgCode + "' and IS_ACTIVE='true'"
    println("Applying filter condition for DQ dataframes --> " + filterCondition)
    val dq_rules_df = DKStorageManager.createDFfromCSV(rules_file_hdfs_location, spark, filterCondition)
    //println("DQ rules dataframe..")
    //dq_rules_df.show(false)
    dq_rules_df
  }

  /**
   * this functions returns the distinct entity names from rules temp table,
   * concerts the DataFrame into array of string of unique names
   */
  
  def getDistinctEntityNamesFromRulesConfigTable(spark: SparkSession) = {
    import spark.implicits._
    spark.sql("select distinct ENTITY_NAME FROM dk_dq_rules")
      .map(row => row.get(row.fieldIndex("ENTITY_NAME")).toString())
      .collect()
  }

  /**
   * the function to map every entity from parsedMap with
   * each row from config dataframe which has mapping rule for
   * every entity
   * dataframe<rule> from config-file --> entity of parsedMap 
   */
  def getEntityWithConfigrowMapping(mappingConfigDF: DataFrame, parsedDFMap: iMap[String, DataFrame]): iMap[Row, DataFrame] = {
    val entityConfigMap = Map[Row, DataFrame]().empty
    
    //here if we don't put collect, for-each loop does not work //TODO
    mappingConfigDF.collect.foreach(mappingConfigRow => {
      val entityKey = mappingConfigRow.getString(mappingConfigRow.fieldIndex("C5"))
      if (parsedDFMap.contains(entityKey))
        entityConfigMap += (mappingConfigRow -> parsedDFMap.get(entityKey).get)
      else
        println("No mapping found for entity :" + entityKey)
    })
    entityConfigMap.toMap
  }

  /**
   * creates and saves jobhistory table
   */
  def createAndSaveDkJobHistoryDataframe(spark:SparkSession, argsMap: iMap[String, String]): Unit = {

    val organizationCode = argsMap.getOrElse("orgCode", "default").toString()
    val sparkAppId = spark.sparkContext.getConf.getAppId

    val jobHistory = DkJobHistory(organizationCode,
      argsMap.getOrElse("appName", sparkAppId).toString(),
      argsMap.getOrElse("jobType", None).toString(), "Dharmik")

    spark.conf.set("spark.sql.parquet.binaryAsString", "true") //make binary as string in parquet
    //making rdd of Job history
    val jobHistoryRdd = spark.sparkContext.parallelize(Row(jobHistory.orgCode, jobHistory.jobName, jobHistory.jobType, jobHistory.createdBy) :: Nil)

    val jobHistoryDF = spark.createDataFrame(jobHistoryRdd, Schema.jobHistorySchema)
    println("Saving job history table..")
    jobHistoryDF.write.mode(SaveMode.Append).parquet(Constants.DkJobHistoryLocation)

  }
}  

