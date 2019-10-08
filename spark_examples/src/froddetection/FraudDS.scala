package froddetection

import org.apache.spark.sql.{Dataset,Encoders, SaveMode, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.expressions.WindowSpec

case class Account(number:String,firstName:String, lastName:String)
case class Transaction(id:Long,account:Account,date:java.sql.Date,amount:Double,description:String)

object FraudDS {
  def main(args: Array[String]): Unit = {
    println("********************** Start of Fraud detection DataSet spark job **********************")
    import org.apache.log4j.{ Level, Logger }
    Logger.getLogger("org").setLevel(Level.ERROR)

    System.setProperty("hadoop.home.dir", "D:\\upload\\dharmik\\winutils")

    val spark = SparkSession
      .builder()
      .appName("FD job")
      .master("local[2]")
      .getOrCreate()

    import spark.implicits._
    val financeDsRaw = spark
      .read
      .option("mode", "DROPMALFORMED") //DROPMALFORMED(remove corrupt) FAILFAST, PERMISSIVE (default)
      .json("inputs/finances-small.json")
      .withColumn("Date", to_date($"Date", "MM/dd/yyyy")).as[Transaction]
    
    financeDsRaw.show
    //start with previous 4 rows and end with current rows
    val windowSpecPrevious4account = Window.partitionBy($"AccountNumber").orderBy($"Date").rowsBetween(-4, 0) 
    val rollingAvgPrevious4account = avg($"Amount").over(windowSpecPrevious4account).as("Last4Avg")
    
    val financeDs = financeDsRaw
      .na.drop("all", Seq("ID", "Account", "Amount", "Date", "Description")) //it will drop records with _corrupt_record column
      .na.fill("Unknown", Seq("Description")).as[Transaction] //since na is untyped
      .where($"Amount" =!= 0 || $"Description" === "Unknown")
      .select($"Account.Number".as("AccountNumber").as[String],$"Amount".as[Double],
          $"Date".as[java.sql.Date](Encoders.DATE), //since there are not explicit encoders for date
          $"Description".as[String]) 
      //.selectExpr("Account.Number as AccountNumber", "Amount", "Description", "to_date(`Date`, 'MM/dd/yyyy') as Date") 
      .select($"*", rollingAvgPrevious4account)
      
    println("********************** End of Fraud detection DataSet spark job **********************")
  }
}