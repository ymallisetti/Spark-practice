package com.dk.dq

import scala.collection.JavaConversions.mapAsJavaMap
import scala.collection.immutable.{ Map => iMap }

import org.apache.commons.lang3.text.StrSubstitutor
import org.apache.spark.SparkContext
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.SaveMode
import scala.collection.mutable.ListBuffer
import com.dk.job.builder.DKDataFrameBuilder
import com.dk.util.Constants
import com.dk.util.UtilityFunctions
import org.apache.spark.sql.Row
import com.dk.schema.Schema
import org.apache.spark.sql.SparkSession

object DKDataQuality {

  def applyDQRules(spark: SparkSession, parsedDFMap: iMap[String, DataFrame]): Option[DataFrame] = {
    var finalExceptionToReturn: Option[DataFrame] = None

    //create properties file
    val dqProperties = UtilityFunctions.getOrCreatePropertyConfig(Constants.applicationConfgFileName)
      .getConfig(Constants.dq)

    val dqRulesDf = DKDataFrameBuilder.getDQConfigDataFrame(spark, dqProperties)

    //broadcast the DF and get the reference back
    val broadcasteDqRulesDf = spark.sparkContext.broadcast(dqRulesDf).value

    if (!UtilityFunctions.isDataFrameEmpty(broadcasteDqRulesDf)) {
      //instead use createOrReplaceTempView 
      broadcasteDqRulesDf.createOrReplaceTempView("dk_dq_rules")
      //apply rules on each entity

      val entityNameFilterQuery = "select * from dk_dq_rules where ENTITY_NAME='${entity_name}'"
      var exceptionList = ListBuffer[Row]() 
      DKDataFrameBuilder.getDistinctEntityNamesFromRulesConfigTable(spark)
        .foreach(entityName => {

          //collect the rules for each entity and apply the rule 
          var entityLvlExceptionDf: Option[DataFrame] = None
          spark.sql(StrSubstitutor.replace(entityNameFilterQuery, Map("entity_name" -> entityName)))
            .collect()
            .foreach(rule => {
              val ruleLvlExceptionDf: Option[DataFrame] = DKDataQualityHandler.
                applyEachRuleForEntity(rule, parsedDFMap.get(entityName).get, entityName, spark, dqProperties)

              if (ruleLvlExceptionDf.isDefined) {
                ruleLvlExceptionDf.get.rdd.collect.foreach(row => {
                  exceptionList.+=:(row)
                })
              }

            }) //loop over each rule for and entity ends

        }) //loop over each entity ends

       if (!exceptionList.isEmpty) {
        val finalDf = spark.createDataFrame(spark.sparkContext.parallelize(exceptionList), Schema.dkExceptionSchema)
        if (!UtilityFunctions.isDataFrameEmpty(finalDf)) {
          println("Saving exception df")
          finalDf.write.mode(SaveMode.Append).parquet(Constants.DkExceptionsLocation)
        }
      } 

    } else {
      println("ERROR:No rules configured for orgnization code -->" + spark.sparkContext.getConf.get("orgCode"))
    }
    finalExceptionToReturn
  }
}