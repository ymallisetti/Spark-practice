package com.dk.transformation

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Row
import com.dk.util.UtilityFunctions._
import scala.collection.mutable.ListBuffer
import org.apache.spark.ml.feature.SQLTransformer

object DKTransformationHandler {
  def transform(configRow : Row, entityDf: DataFrame) : Map[String, DataFrame] = {
    
    val colMapping ="C8"
    val dataMappingList: List[Mapping] = dataJsonMapping(configRow.getString(configRow.fieldIndex(colMapping)))
    
    val sqlTransformationStatement = generateSqlTransformationStatement(dataMappingList) 
    println("SQL transfromation query ------>")
    println(sqlTransformationStatement)
    
    val sqlTrans = new SQLTransformer().setStatement(sqlTransformationStatement)
    val destinationDF = sqlTrans.transform(entityDf)

    Map("dk_data"->destinationDF) //TODO change this hard coding
  }
  
  def generateSqlTransformationStatement(mapping: List[Mapping]): String = {
    var query = new StringBuilder("SELECT ")
    val size = mapping.size
    var srcColList: ListBuffer[String] = new ListBuffer[String]
    mapping.zipWithIndex.foreach {
      case (mapping, index) =>
        mapping.srcColName.split(",").foreach { x => srcColList.append(x) }
        query.append(mapping.srcColExpr).append(" AS ").append(mapping.destColName)
        if (size - 1 != index) { query.append(",") } else { query.append(" FROM __THIS__") }
    }
    query.toString

  }
}