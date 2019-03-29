package examples

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.DataFrame
import util.SparkUtil._
import org.apache.spark.sql.expressions.Window

case class Status(id: Int, customer: String, status: String)
case class CareerHistory(id: Int, empname: String, company: String, joiningDate: String)

object DataFrameWindowsOperation {
  def main(args: Array[String]): Unit = {
    println("Start of DataFrameWindowsOperation example")

    val spark = getLocalSparkSession

    simpleWindowsDemo(spark)
    complexWindowsDemo(spark)
    selectFirstRowFromEachGroup(spark)

    println("End of DataFrameWindowsOperation example")
  }

  def simpleWindowsDemo(spark: SparkSession) = {
    val allStatus = List(Status(1, "DK", "applied"), Status(2, "GJ", "form-fill"), Status(3, "DK", "in-progress"),
      Status(4, "DK", "accepted"), Status(5, "GJ", "rejected"), Status(6, "DK", "offer-letter"))
    val statusDf = spark.createDataFrame(allStatus)

    import spark.implicits._
    val windowSpec = Window.partitionBy($"customer").orderBy($"id")
    val winDf = statusDf.select($"*", lag($"status", 1, "NA").over(windowSpec).as("prevstatus"))
    winDf.show
  }

  def complexWindowsDemo(spark: SparkSession) = {
    val careerData = List(CareerHistory(1, "DK", "10Soft", "2003-05-12"), CareerHistory(2, "Biru", "Mastek", "2006-09-20"),
      CareerHistory(3, "DK", "CT", "2017-02-25"), CareerHistory(4, "DK", "Cogni", "2012-01-21"), CareerHistory(5, "Biru", "Accenture", "2008-08-22"),
      CareerHistory(6, "DK", "12Soft", "2005-05-10"), CareerHistory(7, "DK", "DDULtd", "2005-09-18"), CareerHistory(8, "DK", "TCS", "2009-11-12"))

    import spark.implicits._
    val careerDataDf = spark.createDataFrame(careerData).withColumn("joiningDate", to_date($"joiningDate")) //change string to date

    val windowSpec = Window.partitionBy($"empname").orderBy($"joiningDate") //define window spec object

    //taking the joining date of lead row and subtracting with 1 to put as last date column value
    val windowedCareerDf = careerDataDf
      .select($"*", lead(date_sub($"joiningDate", 1), 1).over(windowSpec).as("lastDate"))
      .withColumn("companyNum", dense_rank().over(windowSpec))
      .withColumn("duration", datediff($"lastDate", $"joiningDate"))

    windowedCareerDf.show
    println("creating admission date and release date for data")

  }

  def selectFirstRowFromEachGroup(spark: SparkSession) = {
    import spark.implicits._
    val df = spark.createDataFrame(Seq(
      (0, "cat26", 30.9), (0, "cat13", 22.1), (0, "cat95", 19.6), (0, "cat105", 1.3),
      (1, "cat67", 28.5), (1, "cat4", 26.8), (1, "cat13", 12.6), (1, "cat23", 5.3),
      (2, "cat56", 39.6), (2, "cat40", 29.7), (2, "cat187", 27.9), (2, "cat68", 9.8),
      (3, "cat8", 35.6))).toDF("Hour", "Category", "TotalValue")

    val wspec = Window.partitionBy($"hour").orderBy($"TotalValue".desc)

    val dfTop = df.withColumn("rowNum", row_number.over(wspec)).where($"rowNum" === 1).drop("rowNum")

    dfTop.show
  }

  /**
   *  +---+--------+------------+-----------+
   * |  2|      GJ|   form-fill|         NA|
   * |  5|      GJ|    rejected|  form-fill|
   * |  1|      DK|     applied|         NA|
   * |  3|      DK| in-progress|    applied|
   * |  4|      DK|    accepted|in-progress|
   * |  6|      DK|offer-letter|   accepted|
   * +---+--------+------------+-----------+
   *
   * +---+-------+---------+-----------+----------+----------+--------+
   * | id|empname|  company|joiningDate|  lastDate|companyNum|duration|
   * +---+-------+---------+-----------+----------+----------+--------+
   * |  2|   Biru|   Mastek| 2006-09-20|2008-08-21|         1|     701|
   * |  5|   Biru|Accenture| 2008-08-22|      null|         2|    null|
   * |  1|     DK|   10Soft| 2003-05-12|2005-05-09|         1|     728|
   * |  6|     DK|   12Soft| 2005-05-10|2005-09-17|         2|     130|
   * |  7|     DK|   DDULtd| 2005-09-18|2009-11-11|         3|    1515|
   * |  8|     DK|      TCS| 2009-11-12|2012-01-20|         4|     799|
   * |  4|     DK|    Cogni| 2012-01-21|2017-02-24|         5|    1861|
   * |  3|     DK|       CT| 2017-02-25|      null|         6|    null|
   * +---+-------+---------+-----------+----------+----------+--------+
   * 
   *+----+--------+----------+
    |Hour|Category|TotalValue|
    +----+--------+----------+
    |   1|   cat67|      28.5|
    |   3|    cat8|      35.6|
    |   2|   cat56|      39.6|
    |   0|   cat26|      30.9|
    +----+--------+----------+
   */
}