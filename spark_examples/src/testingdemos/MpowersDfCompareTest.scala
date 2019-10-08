/*package testingdemos

import org.apache.spark.sql.SparkSession
import org.scalatest.BeforeAndAfterAll
import org.scalatest.FunSpec
import dfcompare.DatasetComparer
import org.apache.spark.sql.types._
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions._
import crap.StandardizationExamples._
import crap.Crap
import util.SparkUtil._
import examples.DataFrameTransformation

class MpowersDfCompareTest extends FunSpec with SparkSessionTestWrapper with DatasetComparer {
  import spark.implicits._

  it("aliases a DataFrame") {

    val sourceDF = Seq(
      ("jose"),
      ("li"),
      ("luisa")).toDF("name")

    val actualDF = sourceDF.select(col("name").alias("student"))

    val expectedDF = Seq(
      ("jose"),
      ("li"),
      ("luisa")).toDF("student")

    assertSmallDatasetEquality(actualDF, expectedDF)

  }

  it("dataframe standarization should be equal") {
    val inputDF = Seq(("Dharmik", "M", "O")).toDF("Name", "Gender", "Medi")
    val expectedDF = Seq(("Dharmik", "Male", "OutPatient")).toDF("Name", "Gender", "Medi")
    val codes = Seq(("M", "Male"), ("F", "Female"), ("O", "Others")).toDF("BeforeCode", "AfterCode")
    //val actualDF = standardiseDfWithJoins(inputDF, codes, spark)
    val actualDF = standardiseDfwithUdf(inputDF)
    actualDF.show
    assertSmallDatasetEquality(actualDF, expectedDF)
  }

  it("transformation of dataframe should be equal") {

   val inuputDF= List(PPat("AAA03", "Virat", "Kohli", "Delhi", "110000", "M", "Delhi","")).toDF
   
   val mapping = List(Mapping("Mariz_ki_id","","PAT_ID","", "COLUMN_NAME_CHANGE", ""),
                     Mapping("SOURCE_NAME","","", "","CONSTANT", "EPIC"),
                     Mapping("ZIP_CODE","Int","ZIP_CODE", "String","", ""))
   
   val expectedDF = Seq( ("AAA03", "Virat", "Kohli", "Delhi", "110000", "M", "Delhi","EPIC",""))
                       .toDF("Mariz_ki_id","FIRST_NAME","LAST_NAME","CITY","ZIP_CODE","GENDER","STATE","SOURCE_NAME","BIRTH_DATE")
                       .withColumn("ZIP_CODE", col("ZIP_CODE").cast(IntegerType))
                       
   //expectedDF.show
   //expectedDF.printSchema()
                       
   val actualDFRaw = DataFrameTransformation.transformDF(inuputDF, mapping, spark)
   val columns = expectedDF.columns
   val actualDF = actualDFRaw.select(columns.head, columns.tail:_*) //rearranging columns of dataframe
   
   //actualDF.show
   //actualDF.printSchema()
   assertSmallDatasetEquality(actualDF, expectedDF)
  }
}

*/