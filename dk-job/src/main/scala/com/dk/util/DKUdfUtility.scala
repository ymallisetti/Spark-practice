package com.dk.util

import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.Row
import scala.reflect.runtime.universe._
import org.apache.spark.sql.{SaveMode,SparkSession}

object DKUdfUtility {

  case class UdfOutput(errorColName: String, errorColValue: String, errorDesc: String)

  def registerAllUdfs(spark: SparkSession) {
    udfNullCheck(spark)

  }

  /**
   * udf to handle null check
   * data type for cols is changed to Object from String while 
   * migrating spark 1.6.1 to 2.1.0
   */
  def udfNullCheck(spark: SparkSession) {
    spark.udf.register("null_check", (r: Row, cols: Object) => {
      val colNames = cols.toString().split(",")
      val resultStatus = new StringBuilder()
      val colValue = new StringBuilder("")
      var count: Int = 0;
      r.toSeq.foreach { x =>
        if (x == null || x.equals("")) { resultStatus.append(colNames(count)).append(" , ") }
        count = count + 1
      }
      if (!resultStatus.isEmpty) {
        resultStatus.replace(resultStatus.length - 2, resultStatus.length - 1, "")
      }
      UdfOutput(resultStatus.toString(), colValue.toString(), s"columns contains the null values")
    })
  }

}