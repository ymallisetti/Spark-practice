package examples

import util.SparkUtil._
import org.apache.spark.sql.functions._
import org.apache.avro.ipc.specific.Person
import org.apache.spark.sql.Encoders
import org.apache.spark.sql.types.TimestampType
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SparkSession

object NestedJsonDenormalize {

  val joinColumn = "PatientDetail.PatientMRN"
  val joinColumnName = "PatientMRN"

  def main(args: Array[String]): Unit = {
    println("Start of NestedJsonDenormalize nested case class demo example")

    val spark = getLocalSparkSession
    import spark.implicits._

    val hl7DF = spark
      .read
      .option("mode", "DROPMALFORMED")
      .json("inputs/hl7-msg.json")
    hl7DF.show(false)

    val patientDF = getSimpleSegmentDF(hl7DF, "PatientDetail", false, spark)
    val encounterDF = getSimpleSegmentDF(hl7DF, "PatientVisitInformation", true, spark)
    val diagnosisDF = getSegmentDF(hl7DF, "PatientDiagnosisInformation", spark)
    patientDF.show(false)
    encounterDF.show(false)
    diagnosisDF.show(false)

    println("End of NestedJsonDenormalize nested case class demo example")
  }

  def getSegmentDF(rawDF: DataFrame, segmentName: String, spark: SparkSession): DataFrame = {
    val segmentAlias = "PatSeg"
    import spark.implicits._

    val diagnosisColumns = List("DiagnosisCode", "DiagnosisCount", "DiagnosisDate/Time", "DiagnosisDescription", "DiagnosisType")
    //since diagnosis segment contains array of StructType, first use explode to unpack array column
    val explodedSegment = rawDF.select(explode(col(segmentName)).alias(segmentAlias), col(joinColumn))
    val segCols = explodedSegment.select(segmentAlias + ".*").schema.fieldNames.toList
    //select all the element of StructType for that column
    explodedSegment.select(segmentAlias + ".*", joinColumnName)
  }

  def getSimpleSegmentDF(rawDF: DataFrame, segmentName: String, nestedSegment: Boolean, spark: SparkSession): DataFrame = {
    import spark.implicits._

    val segmentCols = rawDF.select(segmentName + ".*").schema.fieldNames.toList //get column names
    val extractedDF = if (nestedSegment) {
      rawDF.select(segmentName + ".*", joinColumn) //add join column from the requested nested segment
    } else {
      rawDF.select(segmentName + ".*")
    }
    extractedDF
  }

}