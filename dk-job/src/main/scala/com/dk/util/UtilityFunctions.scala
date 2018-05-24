package com.dk.util

import com.typesafe.config.{ Config, ConfigFactory }
import org.apache.spark.sql.DataFrame
import scala.collection.mutable.ListBuffer
import scala.util.parsing.json.JSON
import org.apache.spark.sql.types.{StringType,StructField,StructType}
import scala.collection.JavaConversions.mapAsScalaMap

object UtilityFunctions {

  @transient private var propertyConfigFile: Config = _
  
  /**
   * method for creating property file instance
   */
  def getOrCreatePropertyConfig(fileName: String): Config = {
    if (propertyConfigFile == null) {
      val configFile = new StringBuilder(fileName).toString()
      propertyConfigFile = ConfigFactory.load(fileName).resolve()
    }
    propertyConfigFile
  }
  
  def getOrCreatePOSchema(fileName: String): Map[String, StructType] = {
    val schemaMap = scala.collection.mutable.Map[String, StructType]().empty
    if (schemaMap.isEmpty) {
      propertyConfigFile = getOrCreatePropertyConfig(fileName)
      
      propertyConfigFile.getObject(Constants.sectionSchemaVarName).foreach({
        case (k, v) => schemaMap.+=(k -> StructType(v.unwrapped().toString().split('|').map(colName =>
          if (colName.contains(" ")) StructField(colName.replaceAll("\\s", "_"), StringType, true) else StructField(colName, StringType, true))))
      })
    }
    schemaMap.toMap
  }
  
  /**
   * this function checks if the dataframe is empty or not
   */
  def isDataFrameEmpty(df:DataFrame): Boolean = {
    df.take(1).isEmpty
  }
  
  def dataJsonMapping(jsonStr:String) : List[Mapping] = {//mapping
    
    var mapping: ListBuffer[Mapping] = new ListBuffer[Mapping]
    val jsonObject = JSON.parseFull(jsonStr)
    val rootObject = jsonObject.get.asInstanceOf[Map[String, Any]]
    var mappingList = rootObject.get("mapping").get.asInstanceOf[List[Map[String, String]]]
    
    mappingList.foreach(mappingMap => {
      mapping.append(Mapping(mappingMap.get("srcColExpr").get,
                            mappingMap.get("destColName").get,
                            mappingMap.get("destColDataType").get,
                            mappingMap.get("srcColName").get,
                            mappingMap.get("srcColDataType").get))
    })
    mapping.toList
  }
  
  case class Mapping(srcColExpr: String, destColName: String, destColDataType: String, srcColName: String, srcColDataType: String)

}