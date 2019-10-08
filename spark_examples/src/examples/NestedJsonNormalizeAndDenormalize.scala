package examples

import util.SparkUtil._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SparkSession

object NestedJsonNormalizeAndDenormalize {

  val joinColumn = "PatientDetail.PatientMRN"
  val joinColumnName = "PatientMRN"

  def main(args: Array[String]): Unit = {
    println("Start of NestedJsonNormalizeAndDenormalize example")
    val spark = getLocalSparkSession

    val hl7DF = getRawJsonDF(spark,"hl7-msg.json")
    hl7DF.show(false)

    val entityTuple = normalizeJsonDocumentToEntities(hl7DF, spark)
    printAllEntities(entityTuple)

    val denormalizedJsonDF = denormalizeEntitiesToJsonDocument(entityTuple)
    denormalizedJsonDF.show(false)
    denormalizedJsonDF.write.json("D:\\junk\\nested")

    println("End of NestedJsonNormalizeAndDenormalize example")
  }

  /**
   * the function will extract the embeded segment of PatientVisit 
   * and Diagnosis from the JSON (DataFrame)
   */
  def normalizeJsonDocumentToEntities(hl7DF: DataFrame, spark: SparkSession) = {
    import spark.implicits._
    val metadataDF = hl7DF.select($"DateTimeMessage", $"HL7Version", $"MessageID", $"PatientDetail.PatientMRN")

    val patientDFRaw = getSimpleSegmentDF(hl7DF, "PatientDetail", spark)
    val patientVisitInformationRaw = getNestedSegmentAsDF(patientDFRaw, "PatientVisitInformation", spark)
    val patientDiagnosisInformation = getNestedSegmentAsDF(patientVisitInformationRaw, "PatientDiagnosisInformation", spark)

    val patientDF = patientDFRaw.drop("PatientVisitInformation") //remove the extracted DF column
    val patientVisitInformationDF = patientVisitInformationRaw.drop("PatientDiagnosisInformation") //remove the extracted DF column

    (patientDF, patientVisitInformationDF, patientDiagnosisInformation)
  }

  /**
   * the function will combine entities Patient, PatientVisit and Diagnosis to form
   * one single nested JSON document
   */
  def denormalizeEntitiesToJsonDocument(tuple: (DataFrame, DataFrame, DataFrame)) : DataFrame= {
    println("De normalize process started")
    val patientDF = tuple._1
    val patientVisitDF = tuple._2
    val diagnosisDF = tuple._3

    //get the joined dataframe with group by and collecting the left DF as list
    val visitDiagJoinedDF = patientVisitDF.join(diagnosisDF, Seq("PVID", "PatientMRN"), "left")
      .groupBy("PVID", "PatientMRN") //need to provide all the columns in 'Enc' schema
      .agg(collect_list(struct(diagnosisDF.columns.map(col): _*)).alias("diagnosis"))
    //after group by aggregation, not all columns are returned, so join the DF again
    val patientVisitDiagnosisCompleteDF = patientVisitDF.join(visitDiagJoinedDF, Seq("PVID", "PatientMRN"), "left")

    val patVisitJoinedDF = patientDF.join(patientVisitDiagnosisCompleteDF, Seq("PatientMRN"), "left")
      .groupBy("PatientMRN") //need to provide all the columns in 'Enc' schema
      .agg(collect_list(struct(patientVisitDiagnosisCompleteDF.columns.map(col): _*)).alias("patientvisit"))
    val patientDFCompleteDF = patientDF.join(patVisitJoinedDF, Seq("PatientMRN"), "left")
    patientDFCompleteDF
  }

  def getSimpleSegmentDF(rawDF: DataFrame, segmentName: String, spark: SparkSession): DataFrame = {
    /*
     * if HL7 message does not have nested Patient information, means only in the root, 
     * then instead of selected Patient columns one by one, just identify header columns 
     * to drop and we will get PatientRawDF as expected
     * 
     * val headerCols = List("HL7Version","MessageID","MessageType","DestinationSystem","DateTimeMessage","SourceSystem")
    	 hl7DF.drop(headerCols:_*).show(false)
     */
    import spark.implicits._
    val segmentCols = rawDF.select(segmentName + ".*").schema.fieldNames.toList //get column names
    rawDF.select(segmentName + ".*")
  }

  def getNestedSegmentAsDF(rawDF: DataFrame, segmentName: String, spark: SparkSession): DataFrame = {
    val segmentAlias = "PatSeg"
    import spark.implicits._

    //since diagnosis segment contains array of StructType, first use explode to unpack array column
    var explodedSegment = rawDF
    var resultDF = rawDF
    if (segmentName.contains("Diagnosis")) { //add the patient visit id in case of diagnosis
      explodedSegment = rawDF.select(explode(col(segmentName)).alias(segmentAlias), col(joinColumnName), col("PVID"))
      resultDF = explodedSegment.select(segmentAlias + ".*", joinColumnName, "PVID")
    } else {
      explodedSegment = rawDF.select(explode(col(segmentName)).alias(segmentAlias), col(joinColumnName))
      resultDF = explodedSegment.select(segmentAlias + ".*", joinColumnName)
    }
    resultDF
  }

  def printAllEntities(tuple: (DataFrame, DataFrame, DataFrame)) = {
    tuple._1.show(false)
    tuple._2.show(false)
    tuple._3.show(false)
  }

  def getRawJsonDF(spark: SparkSession,fileName:String): DataFrame = {
    spark
      .read
      .option("mode", "DROPMALFORMED")
      .json("inputs/jsons/"+fileName)
  }
  
}