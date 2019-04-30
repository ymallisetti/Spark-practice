package froddetection

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.expressions.WindowSpec

object Fraud {
  def main(args: Array[String]): Unit = {
    println("********************** Start of Fraud detection spark job **********************")
    import org.apache.log4j.{ Level, Logger }
    Logger.getLogger("org").setLevel(Level.ERROR)

    System.setProperty("hadoop.home.dir", "D:\\upload\\dharmik\\winutils")

    val spark = SparkSession
      .builder()
      .appName("FD job")
      .master("local[1]")
      .getOrCreate()

    val financeDfRaw = spark
      .read
      .option("mode", "PERMISSIVE") //DROPMALFORMED(remove corrupt) FAILFAST, PERMISSIVE (default)
      .json("inputs/finances-small.json").cache

    import spark.implicits._

    //start with previous 4 rows and end with current rows
    val windowSpecPrevious4account = Window.partitionBy($"AccountNumber").orderBy($"Date").rowsBetween(-4, 0) 
    val rollingAvgPrevious4account = avg($"Amount").over(windowSpecPrevious4account).as("Last4Avg")

    val financeDf = financeDfRaw
      .na.drop("all", Seq("ID", "Account", "Amount", "Date", "Description")) //it will drop records with _corrupt_record column
      .na.fill("Unknown", Seq("Description")) //if description is null, fill with unknown value
      .where($"Amount" =!= 0 || $"Description" === "Unknown")
      .selectExpr("Account.Number as AccountNumber", "Amount", "Description", "to_date(`Date`, 'MM/dd/yyyy') as Date") //extracting columns from nested Account Struc
      .select($"*", rollingAvgPrevious4account)

    //financeDf.write.mode(SaveMode.Overwrite).parquet("D:\\upload\\spark_data\\crap\\frod\\clean")
    financeDf.write.mode(SaveMode.Overwrite).csv("D:\\upload\\spark_data\\crap\\frod\\clean")
    financeDf.show(false)

    println("********************** End of Fraud detection spark job **********************")
  }

  def getAccountStatistics(financeDf: DataFrame, spark: SparkSession): DataFrame = {
    println("Printing account summary")
    import spark.implicits._

    val accountSummary = financeDf
      .select($"AccountNumber", $"Amount", $"Description", to_date($"Date", "MM/dd/yyyy"))
      .groupBy($"AccountNumber")
      .agg(avg("Amount").as("AverageTransaction"),
        sum("Amount").as("TotalTransaction"),
        count("Amount").as("NumberOfTransaction"),
        max("Amount").as("MaxTransaction"),
        collect_set($"Description").as("UniqueDescription"))
      .coalesce(5) //to reduce number of task from default 200 to 5s
    accountSummary.show(false)
    //we can use explode function to get back all the description mapped to that account number so 
    //EXPLODE is inverse of group by clause ---- IMP
    accountSummary
  }

  def handleCorruptRecords(df: DataFrame, spark: SparkSession) = {
    //checking if the _corrup_records column exists or not
    import spark.implicits._
    if (df.hasColumn("_corrupt_record")) { //using implicit class functions
      println("Extracting corrupt records and saving into other location")
      df.filter($"_corrupt_record".isNotNull).select("_corrupt_record")
        .write.mode(SaveMode.Overwrite).option("header", true).csv("D:\\upload\\spark_data\\crap\\frod\\corrupt")
    }

    implicit class DataFrameHelper(df: DataFrame) {
      import scala.util.Try //to avoid AnalysisException
      def hasColumn(column: String) = Try(df(column)).isSuccess
    }
  }

}