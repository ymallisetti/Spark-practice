
package com.dk.schema

import org.apache.spark.SparkContext
import org.apache.spark.sql.{ DataFrame, Row }
import org.apache.spark.sql.types.{ StructType, StructField, StringType, LongType, TimestampType }

import java.text.SimpleDateFormat
import java.util.Date

object Schema {
  val jobHistorySchema = StructType(
    StructField("ROWKEY", StringType, true) ::
      StructField("C1", StringType, true) ::
      StructField("C2", StringType, true) ::
      StructField("C15", StringType, true) :: Nil)

   val dkExceptionSchema = StructType(Array(
    StructField("org_code", StringType, true),
    StructField("application_id", StringType, true),
    StructField("entity_name", StringType, true),
    StructField("seq_id", StringType, true),
    StructField("rule_pattern", StringType, true),
    StructField("error_code", StringType, true),
    StructField("error_description", StringType, true),
    StructField("status", StringType, true),
    StructField("created_datetime", TimestampType, true),
    StructField("scry_role_cd", StringType, true),
    StructField("aud_load_id", StringType, true),
    StructField("aud_load_tm", StringType, true) ))
}
  
  
