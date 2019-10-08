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
import java.text.SimpleDateFormat
import scala.util.control._
import scala.util.Try

object DataFrameTransformation {

  //the case classes are defined in the SparkUtil as it is used in Unit test cases
  //case class Mapping(DESTINATION_COL_NAME: String, DESTINATION_DATATYPE: String,SOURCE_COL_NAME: String, SOURCE_DATATYPE: String,TRANSFORM_TYPE: String, SOURCE_EXPRESSION: String)
  //case class PPat(PAT_ID: String, FIRST_NAME: String, LAST_NAME: String, CITY: String, ZIP_CODE: String, GENDER: String, STATE: String, BIRTH_DATE:String)
  val codes = Seq(("M", "Male"), ("F", "Female"), ("O", "Others"), ("A", "Asian"), ("E", "English"),
                  ("MA", "Married"), ("S", "Single"), ("H", "Hindu"), ("J", "Jewish"),("Y","Yes"))

  val date_formats = List("yyyy/MM/dd", "dd/MM/yyyy", "ddMMyyyy", "dd-MMM-yyyy", "dd-MM-yyyy")

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
          case "CONSTANT"        => tempDF = tempDF.withColumn(mapping.DESTINATION_COL_NAME, lit(mapping.SOURCE_EXPRESSION))
          case "FUNCTION"        => tempDF = performFunctionTransformation(mapping, tempDF, spark)
          case "STANDARDIZATION" => tempDF = codeStandardization(mapping, tempDF, spark)
          case _                 => tempDF
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
    readDataFrame(spark, "inputs/patient_transformation_test.csv")
  }

  val performFunctionTransformation = (mapping: Mapping, srcDF: DataFrame, spark: SparkSession) =>
    srcDF.withColumn(mapping.DESTINATION_COL_NAME, expr(mapping.SOURCE_EXPRESSION))

  /**
   * to handle cases
   * when cast of column does not work and gives null values but not an exception
   * handle actual exception when parsing Dates and TimeStamp
   */
  def castDestinationToDataType(transformedDF: DataFrame, mapping: Mapping, spark: SparkSession): DataFrame = {
    import scala.util.Try

    //register the UDF to be used when coolumn type is date
    spark.udf.register("toDateFormatType", dateFormatUdf)

    val destDataType = mapping.DESTINATION_DATATYPE
    if (destDataType.equalsIgnoreCase("Date"))
      transformedDF.withColumn(mapping.DESTINATION_COL_NAME, expr("toDateFormatType(" + mapping.SOURCE_COL_NAME + ")"))
    else if (transformedDF.hasColumn(mapping.DESTINATION_COL_NAME) && !destDataType.isEmpty)
      transformedDF.withColumn(mapping.DESTINATION_COL_NAME, col(mapping.DESTINATION_COL_NAME).cast(destDataType))
    else transformedDF
  }

  /**
   * cast can directly work on Int or IntegerType etc
   * same with Long or LongType
   */
  def getSparkDataTypeFromDBDataType(dbDataType: String): DataType = {
    dbDataType match {
      case "Int"  => IntegerType
      case "Long" => LongType
      case _      => StringType
    }
  }

  /**
   * UDF to be used for formatting dates for the supplied column values for each row.
   * The UDF will scan for each availbale date formats and try to parse the date and
   * returns the first not null dates successfully parsed.
   */
  val loop = new Breaks;
  val dateFormatUdf = udf((dateSample: String) => {
    var date: java.sql.Date = null
    loop.breakable {
      date_formats foreach { date_format =>
        if (date_format.length.equals(dateSample.length)) { //to optimize and filter dates
          val sdf = new SimpleDateFormat(date_format)
          sdf.setLenient(false)
          val parsedDate = Try(sdf.parse(dateSample))
          if (parsedDate.isSuccess) {
            date = new java.sql.Date(parsedDate.get.getTime)
            loop.break
          }
        } //if condition
      } // for each
    }
    date
  })

  def codeStandardization(mapping: Mapping, df: DataFrame, spark: SparkSession): DataFrame = {
    import spark.implicits._
    val codesDF = codes.toDF("BeforeCode", "AfterCode")
    df.join(codesDF, df(mapping.SOURCE_COL_NAME) === codesDF("BeforeCode"), "left") //to broadcast the code list
      .drop("BeforeCode", mapping.SOURCE_COL_NAME)
      .withColumnRenamed("AfterCode", mapping.DESTINATION_COL_NAME)
  }

  def getSourcePatient = {
    List(PPat("10001", "Joe", "Root", "Birmingham", "00089", "M", "Wales", "1988/02/20"),
      PPat("10002", "Jason", "Holder", "Cape Guena", "100000", "M", "Test State", "17-09-1967"),
      PPat("10003", "Virat", "Kohli", "Delhi", "110000", "M", "Delhi", "12-JAN-2000"))
  }

  implicit class DataFrameHelper(df: DataFrame) {
    import scala.util.Try //to avoid AnalysisException
    def hasColumn(column: String) = Try(df(column)).isSuccess
  }
}