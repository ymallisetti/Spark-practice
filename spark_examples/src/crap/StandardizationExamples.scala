package crap

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SaveMode
import java.nio.charset.StandardCharsets
import org.apache.spark.ui.SparkUI
import util.SparkUtil
import org.apache.spark.sql.Row
import org.apache.spark.TaskContext
import org.apache.spark.sql.Encoders
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StringType
import scala.collection.mutable.ListBuffer

object StandardizationExamples {

  val genders = Map("M" -> "Male", "F" -> "Female", "O" -> "Others")
  val medics = Map("I" -> "InPatient", "O" -> "OutPatient")

  val genderUdf = udf((genderInp: String) => genders.get(genderInp))
  val mediUdf = udf((mediInpt: String) => medics.get(mediInpt))

  def main(args: Array[String]): Unit = {
    import org.apache.log4j.{ Level, Logger }

    Logger.getLogger("org").setLevel(Level.ERROR)

    System.setProperty("hadoop.home.dir", "D:\\upload\\dharmik\\winutils")
    val spark = SparkSession.builder().master("local[*]").getOrCreate

    import spark.implicits._

    val df = Seq(("Dharmik", "M", "O"), ("Gunjan", "M", "O"), ("Sonia", "F", "I")).toDF("Name", "Gender", "Medi")
    val codes = Seq(("M", "Male"), ("F", "Female"), ("O", "Others")).toDF("BeforeCode", "AfterCode")
    println("Source dataframe")
    df.show

    println("After applying UDF")
    standardiseDfwithUdf(df).show

    println("After applying Function")
    df.transform(standarizeDfWithCases()).show

    println("After joining two dataframes Function")
    standardiseDfWithJoins(df, codes, spark).show

    //using mappers
    val res = df.map(r => (r.getString(0), genders.get(r.getString(1)), medics.get(r.getString(2))))
  }

  def standardiseDfWithJoins(df: DataFrame, codes: DataFrame, spark: SparkSession): DataFrame = {
    df.join(codes, df("Gender") === codes("BeforeCode"), "left").show
    df
  }

  def standardiseDfwithUdf(df: DataFrame): DataFrame = {
    val resDf = df.withColumn("Gender", genderUdf(col("Gender")))
      .withColumn("Medi", mediUdf(col("Medi")))
    resDf
  }

  def standarizeDfWithCases()(df: DataFrame): DataFrame = {
    val resDf = df.withColumn("Gender", when(col("Gender").===("M"), "Male").when(col("Gender").===("F"), "Female").otherwise("Not Specified"))

    resDf
  }

}