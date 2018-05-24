package com.dk.job

import com.dk.parser.ParserDriver
import scala.collection.JavaConversions.asScalaBuffer //imp to convert java collections to scala
import scala.collection.JavaConversions.mapAsScalaMap //imp to convert java map to scala
import scala.collection.mutable
import org.apache.spark.sql.Row

object DKParserWrapper {


  def parsePurchaseOrderMsgs(fileName: String, fileContent: String):List[(String, List[Row])] = {
    var resultList = mutable.ListBuffer[(String, List[Row])]()
    //call parser for individual file content and will return map for all entities and values
    val parsedPODataRaw = ParserDriver.parsePOXmls(fileName, fileContent).toMap

    parsedPODataRaw.foreach(parsedData => {
      val scalaRowList = mutable.ListBuffer[Row]()
      parsedData._2.foreach(row => {
        val cleanedRow = row.map(str => Option(str).getOrElse("").toString().trim())
        scalaRowList.+=:(Row.fromSeq(cleanedRow.toSeq))
      })
      val tuple = (parsedData._1, scalaRowList.toList)
      resultList.+=:(tuple)
    })
    resultList.toList
  }
}