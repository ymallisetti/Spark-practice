package examples

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

object DataFrameTransformation {
  

  def main(args: Array[String]): Unit = {
    println("Start of DataFrameTransformation demo example")

    val spark = getLocalSparkSession
    import spark.implicits._
    spark.udf.register("CONCATUDF", (first: String, second: String) => first + " and " + second)

    val srcDF = getSourcePatient.toDF
    srcDF.show

    val mappings = getTransformationMapping(spark).as[Mapping].collect.toList

    val destDF = transformDF(srcDF, mappings, spark)
    destDF.show

    println("End of DataFrameTransformation demo example")
  }

  def transformDF(srcDF: DataFrame, mappings: List[Mapping], spark: SparkSession) = {
    var tempDF: DataFrame = srcDF
    var columnsToDrop = new ListBuffer[String]()
    mappings.foreach {
      mapping =>
        mapping.TRANSFORM_TYPE match {
          case "COLUMN_NAME_CHANGE" => {
            columnsToDrop += (mapping.SOURCE_COL_NAME)
            tempDF = columnValueMapping(mapping, tempDF, spark)
          }
          case "CONSTANT" => tempDF = tempDF.withColumn(mapping.DESTINATION_COL_NAME, lit(mapping.SOURCE_EXPRESSION))
          case "FUNCTION" => tempDF = performFunctionTransformation(mapping, tempDF, spark)
          case _          => tempDF
        }

    }
    tempDF = tempDF.drop(columnsToDrop: _*)
    tempDF
  }

  def columnValueMapping(mapping: Mapping, srcDF: DataFrame, spark: SparkSession): DataFrame = {
    srcDF.withColumn(mapping.DESTINATION_COL_NAME, col(mapping.SOURCE_COL_NAME))
  }

  def getTransformationMapping(spark: SparkSession): DataFrame = {
    readDataFrame(spark, "inputs/patient_transformation.csv")
  }

  def getSourcePatient = {
    List(PPat("AAA01", "Joe", "Root", "Birmingham", "CX0089", "M", "Wales"),
      PPat("AAA02", "Jason", "Holder", "Cape Guena", "XX0000", "M", "Test State"),
      PPat("AAA03", "Virat", "Kohli", "Delhi", "110000", "M", "Delhi"))
  }

  val performFunctionTransformation = (mapping: Mapping, srcDF: DataFrame, spark: SparkSession) =>
    srcDF.withColumn(mapping.DESTINATION_COL_NAME, expr(mapping.SOURCE_EXPRESSION))


}