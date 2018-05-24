package com.dk.transformation

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Row
import com.dk.util.UtilityFunctions._

object DKTransformationHandler {
  def transform(configRow : Row, entityDf: DataFrame) : Map[String, DataFrame] = {
    
    val colMapping ="C8"
    val dataMappingList: List[Mapping] = dataJsonMapping(configRow.getString(configRow.fieldIndex(colMapping)))
    
    
    //val sqlTransformationStatement = generateSqlTransformationStatement(dataMappingList) //TODO ----------------------->>>>>>>>>>>>>>>>
    
    //TODO dummy
    Map(""->entityDf)
  }
}