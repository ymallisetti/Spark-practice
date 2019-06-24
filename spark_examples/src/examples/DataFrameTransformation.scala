package examples

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SaveMode
import java.nio.charset.StandardCharsets
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
import org.apache.spark.sql.types.DataType
import org.apache.spark.sql.types.LongType
import scala.util.Success
import scala.util.Failure

object DataFrameTransformation {

  //case class Mapping(DESTINATION_COL_NAME: String, DESTINATION_DATATYPE: String,SOURCE_COL_NAME: String, SOURCE_DATATYPE: String,TRANSFORM_TYPE: String, SOURCE_EXPRESSION: String)

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
    destDF.printSchema

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
        tempDF = castDestinationToDataType(tempDF, mapping, spark)
    }

    tempDF = tempDF.drop(columnsToDrop: _*)
    tempDF
  }

  def columnValueMapping(mapping: Mapping, srcDF: DataFrame, spark: SparkSession): DataFrame = {
    val source_column = mapping.SOURCE_COL_NAME
    val tempDF = try {
      srcDF.withColumn(mapping.DESTINATION_COL_NAME, col(source_column))
    } catch {
      case unknown: Exception => {
        println(s"Column not present in source : $source_column")
        srcDF
      }
    }
    tempDF
  }

  def getTransformationMapping(spark: SparkSession): DataFrame = {
    readDataFrame(spark, "inputs/patient_transformation.csv")
  }

  def getSourcePatient = {
    List(PPat("10001", "Joe", "Root", "Birmingham", "00089", "M", "Wales"),
      PPat("10002", "Jason", "Holder", "Cape Guena", "100000", "M", "Test State"),
      PPat("10003", "Virat", "Kohli", "Delhi", "110000", "M", "Delhi"))
  }

  val performFunctionTransformation = (mapping: Mapping, srcDF: DataFrame, spark: SparkSession) =>
    srcDF.withColumn(mapping.DESTINATION_COL_NAME, expr(mapping.SOURCE_EXPRESSION))

  def castDestinationToDataType(transformedDF: DataFrame, mapping : Mapping, Spark: SparkSession): DataFrame = {
      import scala.util.Try
      /**
       * to handle cases
       * when cast of column does not work and gives null values but not an exception
       * handle actual exception when parsing Dates and timestamp
       */
      if (transformedDF.hasColumn(mapping.DESTINATION_COL_NAME))
        transformedDF.withColumn(mapping.DESTINATION_COL_NAME, col(mapping.DESTINATION_COL_NAME).cast(mapping.DESTINATION_DATATYPE))
      else transformedDF
  }
    
  def getSparkDataTypeFromDBDataType(dbDataType:String) : DataType = {
    dbDataType match {
      case "Int" => IntegerType
      case "Long" => LongType
    }
  }
  
  implicit class DataFrameHelper(df: DataFrame) {
      import scala.util.Try //to avoid AnalysisException
      def hasColumn(column: String) = Try(df(column)).isSuccess
}
}