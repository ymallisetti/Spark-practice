package com.dk.transformation

import scala.collection.immutable.{ Map => iMap }
import org.apache.spark.SparkContext
import org.apache.spark.sql.DataFrame
import com.dk.storage_manager.DKStorageManager
import com.dk.job.builder.DKDataFrameBuilder
import org.apache.spark.sql.SparkSession

object DKTransformers {
  
  def performTransformation(parsedDFMap: iMap[String, DataFrame], spark: SparkSession) = {
    val transformerConfigDf = DKStorageManager.createDFfromCSV("mapping_config.csv", spark, null)
    val mappingConfEntityMap = DKDataFrameBuilder.getEntityWithConfigrowMapping(transformerConfigDf, parsedDFMap)
    
    try{
      mappingConfEntityMap.foreach(confRow_EntityDfEntry => {
        DKTransformationHandler.transform(confRow_EntityDfEntry._1,confRow_EntityDfEntry._2).foreach(transformedDataMap => {
          println("transformed data --> "+transformedDataMap._2)
          //TODO
          //save to parquet
        })
      })
      
      
    }catch{
      case e: Throwable => {
    	  println("Error occured for applying the tranformations " ,e )
        e.printStackTrace();
      }
    }
    
    println("mapping config -->")
    transformerConfigDf.show(false)
  }
}