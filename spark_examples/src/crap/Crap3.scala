package crap

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SaveMode
import java.nio.charset.StandardCharsets
import org.apache.spark.ui.SparkUI
import util.SparkUtil._
import org.apache.spark.sql.Row
import org.apache.spark.TaskContext
import org.apache.spark.sql.Encoders
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StringType
import scala.collection.mutable.ListBuffer
import org.apache.spark.sql.Column
import java.text.SimpleDateFormat
import scala.util.control._
import org.apache.spark.storage.StorageLevel
import org.apache.spark.sql.types.LongType

object Crap3 {

  def main(args: Array[String]): Unit = {
    println("Start of Crap3")
    val spark = getLocalSparkSession
    import spark.implicits._

    val jsonDFRAW = spark
      .read
      .json("inputs/jsons/json1.json")
    jsonDFRAW.show(false)
    
    val encounter_df = jsonDFRAW
                              .select(
                                   col("PatientInternalId_MRN").as("MRN"),col("PatientVisitInformation.*"))
                                   .drop("PatientDiagnosisInformation")
    println("Encounter data set")
    encounter_df.show(false)
                                   
    val diagnosis_df = jsonDFRAW
                               .select(
                                   col("PatientInternalId_MRN").as("MRN"),col("PatientVisitInformation.PvId").as("PvId"),
                                   explode(col("PatientVisitInformation.PatientDiagnosisInformation")).alias("DIAGNOSIS")
                                   )
                               .select("MRN","PvId","DIAGNOSIS.*")
    println("Diagnosis data set")
    diagnosis_df.show(false)
    
    val visitDiagJoinedDF = encounter_df.join(diagnosis_df, Seq("PvId", "MRN"), "left")
      .groupBy("PvId", "MRN") 
      .agg(collect_list(struct(diagnosis_df.columns.map(col): _*)).alias("diagnosis"))
      
    //after group by aggregation, not all columns are returned, so join the DF again
    val patientVisitDiagnosisCompleteDF = encounter_df.join(visitDiagJoinedDF, Seq("PvId", "MRN"), "left")
    patientVisitDiagnosisCompleteDF.show(false)
    //patientVisitDiagnosisCompleteDF.write.mode(SaveMode.Overwrite).json("D:\\junk\\nested")
    println("End of Crap3")
  }
  
  def getDummyColumn(df: DataFrame) : Column = {
    when(col("PatientClass") === "I", col("Bed"))
          .otherwise(col("Facility"))
  }
  
  
}