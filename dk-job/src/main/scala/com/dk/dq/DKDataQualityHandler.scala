package com.dk.dq

import scala.collection.JavaConversions.mapAsJavaMap
import org.apache.spark.sql.Row
import org.apache.spark.sql.DataFrame
import org.apache.spark.SparkContext
import com.typesafe.config.Config
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.expr
import org.apache.spark.sql.SQLContext
import org.apache.commons.lang3.text.StrSubstitutor
import org.apache.spark.sql.SparkSession

object DKDataQualityHandler {

  val exceptionDfQuery = "select '${orgCode}' AS org_code,'${applicationId}' AS application_id,entity_name,id as seq_id,'${rulePattern}' as rule_pattern,'${errorCode}' AS error_code,  concat_ws ('_',dq_remarks.errorColName,dq_remarks.errorDesc) AS error_description,'FAILED' AS status,current_timestamp() as created_datetime ,'GEN' as scry_role_cd,'DK_PARSER' as aud_load_id,FROM_UNIXTIME(UNIX_TIMESTAMP(),'YYYY-MM-dd')  from DKExceptionsTable"
  def applyEachRuleForEntity(ruleRow: Row, entityDf: DataFrame, entityName: String,
                             spark: SparkSession, properties: Config): Option[DataFrame] = {
    var dummy: Option[DataFrame] = None //TODO to delete

    val columnList = ruleRow.getString(ruleRow.fieldIndex("ATTRIBUTE_LIST"))

    ruleRow.getString(ruleRow.fieldIndex("PATTERN_TYPE")) match {
      case "INVALID_CODE_LOOK_UP" => dummy
      case _ => {
        //for rest all the rules 
        val entityDFWithRemarks = entityDf.withColumn("dq_remarks", expr(ruleRow.getString(ruleRow.fieldIndex("RULE_EXPRESSION")))).persist()
        
        makeExceptionDF(spark, Some(entityDFWithRemarks.filter("dq_remarks.errorColName IS NOT NULL AND dq_remarks.errorColName !=''")),
          ruleRow: Row, columnList: String, entityName: String, exceptionDfQuery, "DQ Exception")
      }
    }

  }

  private def makeExceptionDF(spark: SparkSession, entityOpDf: Option[DataFrame], ruleRow: Row,
                              attribute: String, entityName: String, filterQuery: String, errorCode: String): Option[DataFrame] = {

    var exceptionDF: Option[DataFrame] = None

    if (entityOpDf.isDefined) {
      val catCode = ruleRow.getString(ruleRow.fieldIndex("LOOKUP_CATEGORY"))
      val ecm_code_set_name = if (catCode != null) catCode else ""

      val valueMap = Map("orgCode" -> spark.conf.get("orgCode"), "applicationId" ->
        spark.sparkContext.getConf.getAppId, "DQErrorMessage" -> ruleRow.getString(ruleRow.fieldIndex("ERROR_MESSAGE")),
        "ecm_code_set_name" -> ecm_code_set_name, "DQLookUpColName" -> attribute,
        "errorCode" -> errorCode, "rulePattern" -> ruleRow.getString(ruleRow.fieldIndex("PATTERN_TYPE")))

      entityOpDf.get.createOrReplaceTempView("DKExceptionsTable")

      exceptionDF = Some(spark.sql(StrSubstitutor.replace(filterQuery, valueMap).toString()))
    }
    exceptionDF
  }

}