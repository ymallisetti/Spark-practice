package com.dk.training

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.functions._

/**
 * this demo class is for training and study of
 * spark sql
 */
object FraudDetection {
  def main(args: Array[String]): Unit = {
    println("********************** Start of Fraud detection spark job **********************")

    val spark = SparkSession
      .builder()
      .appName("Fraud detection Dk job")
      .config("spark.driver.memory", "2G")
      .enableHiveSupport()
      .getOrCreate()

    val financeDf = spark.read.json("/dharmik/input_all/finances-small.json")

    import spark.implicits._
    println("Showing clean data of finance")
    val cleanFinanceDf = financeDf
      .na.drop("all", Seq("ID", "Account", "Amount", "Date", "Description")) //for corrupt records, all this columns will be null
      .na.fill("Unknown", Seq("Description")) //if description is null, fill with unknown value
      .where($"Amount" =!= 0 || $"Description" === "Unknown")
      .selectExpr("Account.Number as AccountNumber", "Amount", "Description", "Date") //limits the output columns
    cleanFinanceDf.show(false)


    //function concat will need the import sql.functions, 
    //lit will generate a column with static value, since concat will expect every 
    //parameter as a column object, so we have to wrap a space with a lit
    println("Showing only name and account numbers")
    val accInfoDf = financeDf.
      select( concat($"Account.FirstName", lit(" "), $"Account.LastName").as("FullName"), $"Account.Number".as("AccountNumber") ).distinct
    accInfoDf.show(false)
    
    
    //getting all the account summary with aggregation
    println("Printing account summary")
    val accountSummary = cleanFinanceDf
      .select($"AccountNumber", $"Amount", $"Description", $"Date")
      .groupBy($"AccountNumber")
      .agg( avg("Amount").as("AverageTransaction"), 
            sum("Amount").as("TotalTransaction"),
            count("Amount").as("NumberOfTransaction"),
            max("Amount").as("MaxTransaction"),
            collect_set($"Description").as("UniqueDescription") )
      //.coalesce(5) //to reduce number of task
    accountSummary.show(false)
    //we can use explode function to get back all the description mapped to that account number 
    //so explode is inverse of group by clause ---- IMP
    
    
    println("Searching for account number which is MOVIE goer")
    //UniqueDescription is now a array we can do any array operations like size, sort etc
    val movieGoerDf = accountSummary
      .select($"AccountNumber", $"UniqueDescription", array_contains($"UniqueDescription", "Movies").as("WentToMovies") )
    movieGoerDf.show(false)
      
    
    println("Account with no MOVIE lover")
    val movieGoerNeverDf = accountSummary
      .select($"AccountNumber", $"UniqueDescription", array_contains($"UniqueDescription", "Movies").as("WentToMovies") )
      .where(!$"WentToMovies") //since WentTOMovies is now a boolean value
    movieGoerNeverDf.show(false)
    
            
    //logging and saving the corrupt records
    println("Printing only corrupt records")
    if (financeDf.hasColumn("_corrupt_record")) { //using implicit class functions
      println("the dataframe has corrupt records...please handle")
      financeDf.where($"_corrupt_record".isNotNull).select($"_corrupt_record")
        .write.mode(SaveMode.Overwrite).parquet("/dharmik/tables/finance/")
    }
    
    println("Showing who is Ameer Gareeb and Billionair")
    val ameerGareebDF = cleanFinanceDf
      .select($"*", when($"Amount"<50, "Gareeb").when($"Amount">150, "Billionair").otherwise("MiddleClass") )
    ameerGareebDF.show
    
    FraudDetectionHelperAndDemos.moreFunctionsDemos(spark)

    println("********************* End of Fraud detection spark job *************************")
    println(" ")
  }

  /**
   * implicit class will extend additional functionality to
   * an already closed class like DataFrame
   */
  implicit class DataFrameHelper(df: DataFrame) {
    import scala.util.Try //to avoid analysis exception
    def hasColumn(column: String) = Try(df(column)).isSuccess
  }
}