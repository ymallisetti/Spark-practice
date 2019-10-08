package crap
import util.SparkUtil._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Row

object Crap4 {

  def main(args: Array[String]): Unit = {
    println("Start of Crap4 example")

    val spark = getLocalSparkSession
    
    spark.read.parquet("D:\\junk\\patient_data").show(false)

    /*val masterLayerSchemaCols = List("MEDICAL_RECORD_NUMBER","FIRST_NAME","LAST_NAME","DEATH_OF_DEATH").map(_.toLowerCase)
    val rawLayerSchemaCols = List("MEDICAL_RECORD_NUMBER","FIRST_NAME").map(_.toLowerCase)
    val PatientIgnoreNAColumns = List("death_of_death")

    val res = masterLayerSchemaCols.toList.map(x => x match {
      case x if rawLayerSchemaCols.contains(x) => col(x).as(x)
      case x if PatientIgnoreNAColumns.contains(x) => lit(null).as(x)
      case _ => lit("NA").as(x)
    })
    
    res.foreach(println)*/

    println("End of Crap4 example")
  }

  def desiredIdOne(df: DataFrame, spark: SparkSession) = {
    import spark.implicits._

    val idString = df.select(explode(col("PatientInternalID")).as("exploded_column"))
      .select("exploded_column.IdentifierTypeCode", "exploded_column.ID")
      .filter($"IdentifierTypeCode" === "MRN").select($"ID")
      .collect.head.getAs[String]("ID")
    println(idString)
    idString
  }

  val my_array_size = udf { addresses: Seq[Row] =>
    {
      val primaryAddress = addresses.filter(add => add.fieldIndex("type") > 0).toString
      primaryAddress
    }
  }

  def getRawJsonDF(spark: SparkSession, fileName: String): DataFrame = {
    spark
      .read
      //.option("mode", "DROPMALFORMED")
      //.option("multiLine", true)
      .json("inputs/jsons/" + fileName)
  }

  def crap = {

    //val pDF = hl7DF.selectExpr("addresses[0]").show
    //val addressDF = hl7DF.filter(c).show(false)
    //val c = array_contains(column = $"PatientDemographics.PatientInternalID.IdentifierTypeCode", value = "MRN1")

    val my_array_size = udf { addresses: Seq[Row] =>
      {
        val primaryAddress = addresses.filter(add => add.fieldIndex("type") > 0).toString
        primaryAddress
      }
    }

    val pat_mrn_extractor = udf { addresses: Seq[Row] =>
      for {
        file <- addresses
        if file.fieldIndex("type") > 0
      } yield file.getString(file.fieldIndex("type"))
    }
  }

}