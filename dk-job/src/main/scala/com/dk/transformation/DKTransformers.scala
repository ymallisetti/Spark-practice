package com.dk.transformation

import scala.collection.immutable.{ Map => iMap }
import org.apache.spark.SparkContext
import org.apache.spark.sql.DataFrame
import com.dk.storage_manager.DKStorageManager
import com.dk.job.builder.DKDataFrameBuilder
import org.apache.spark.sql.SparkSession
import com.dk.util.Constants
import org.apache.spark.sql.SaveMode

object DKTransformers {
  
  def performTransformation(parsedDFMap: iMap[String, DataFrame], spark: SparkSession) = {
    val transformerConfigDf = DKStorageManager.createDFfromCSV(Constants.MppingFileLocation, spark, null)
    transformerConfigDf.show(false)
    val mappingConfEntityMap = DKDataFrameBuilder.getEntityWithConfigrowMapping(transformerConfigDf, parsedDFMap)
    
    try{
      mappingConfEntityMap.foreach(confRow_EntityDfEntry => {
        DKTransformationHandler.transform(confRow_EntityDfEntry._1,confRow_EntityDfEntry._2).foreach(transformedDataMap => {
          println("transformed data --> "+transformedDataMap._2.show(false))
          transformedDataMap._2.write.mode(SaveMode.Append).parquet(Constants.DkTablesBaseDir + transformedDataMap._1)
        })
      })
      println("Transformation process completed")
    }catch{
      case e: Throwable => {
    	  println("Error occured for applying the tranformations " ,e )
        e.printStackTrace();
      }
    }
    
  }
}