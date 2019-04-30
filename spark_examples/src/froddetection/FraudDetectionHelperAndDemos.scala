package froddetection

import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.DataFrame

object FraudDetectionHelperAndDemos {
  
  def simpleAnalysisOnData(spark:SparkSession,accountSummary: DataFrame) = {
    import spark.implicits._
    
    println("Account with no MOVIE lover")
    val movieGoerNeverDf = accountSummary
      .select($"AccountNumber", $"UniqueDescription", array_contains($"UniqueDescription", "Movies").as("WentToMovies"))
      .where(!$"WentToMovies") //since WentTOMovies is now a boolean value
    movieGoerNeverDf.show(false)

    println("Showing who is Ameer Gareeb and Billionair")
    val ameerGareebDF = accountSummary
      .select($"*", when($"Amount" < 50, "Gareeb").when($"Amount" > 150, "Billionair").otherwise("MiddleClass"))
    ameerGareebDF.show
  }
  
  def getAccountDistinctAccountNumberAndFullName(financeDfRaw: DataFrame, spark: SparkSession): DataFrame = {
    import spark.implicits._
    println("Showing only name and account numbers")
    val accInfoDf = financeDfRaw.
      select(concat($"Account.FirstName", lit(" "), $"Account.LastName").as("FullName"), $"Account.Number".as("AccountNumber")).distinct
    accInfoDf
  }
  
  def moreFunctionsDemos(spark: SparkSession) {
    val peopleDF = spark.createDataFrame(List(
      Person("Justin", "Pahone", 32, None, Some("Programmer")),
      Person("John", "Smoth", 22, Some(170.0), None),
      Person("Jane ", "Daw", 16, None, None),
      Person(" jane", "Smith", 42, Some(120.0), Some("Chemical Engineer")),
      Person("John", "Daw", 25, Some(222.2), Some("Teacher"))))

    println("People dataframe")
    peopleDF.show

    import spark.implicits._
    
    //aggregate on non agg columns like weight using first
    val incorrectFirstNameGroupDF=peopleDF.groupBy($"firstName").agg(first($"weight"))
    incorrectFirstNameGroupDF.show

    //since first name case is diff and having spaces, we will change the values of firstname colum 
    //by removing spaces and changing the cases, using withcolum which will replace the values of the column. 
    //if with column has same value as DataFrame column, it will replace the values
    val correctedFirstNamePeopleDF = peopleDF.withColumn("firstName", trim(initcap($"firstName")))
    correctedFirstNamePeopleDF.show
    correctedFirstNamePeopleDF.groupBy(lower($"firstName")).agg(first($"weight")).show //now grouping will be correct
    
    println("Sorting the values based on weight") //we can also use functions for sorting instead of column methods
    correctedFirstNamePeopleDF.sort($"weight".desc).groupBy($"firstName").agg(first($"weight")).show
    
    println("handling null values in the weight column and replacing values")
    correctedFirstNamePeopleDF.withColumn("weight",coalesce($"weight",lit(0))).show

    //searching for specific strings in a column values
    println("Printing people who has jobtype as Engineer")
    correctedFirstNamePeopleDF.filter(lower($"jobType").contains("engineer")).show
    
    println("Printing people who has jobtype as Chemical Engineer or Teache")
    //note here isin method takes the argument as var agrs so we are casting/making the list as var args (_*)
    correctedFirstNamePeopleDF.filter(lower($"jobType").isin(List("chemical engineer","teacher"):_*)).show
  }
  
  def udfDemos(spark:SparkSession){
    val sampleDF = spark.createDataFrame(List((1,"This is demo string"),(2,"Yet another demo small"))).toDF("ID","TEXT")
    
    import spark.implicits._
    //registering udf which takes a full string, splits into words using space, capitalize first character of work,
    //and generate a final string from array[string] using a space
    println("Printing dataframe columns with a help of udf functions")
    spark.udf.register("capitalizeFirstLetterOfAllWords", (fullString:String)=>fullString.split(" ").map(_.capitalize).mkString(" "))
    sampleDF.select($"ID",callUDF("capitalizeFirstLetterOfAllWords",$"TEXT")).show(false)
    
    //or we can use UDF in other way
    //this time with one extra parameter for delemeter type for making string
    println("udf function with two arguments ")
    val myUDFwithTwoArgs = udf( (fullString:String,splitter:String)=>fullString.split(" ").map(_.capitalize).mkString(splitter) )
    //we need to specifiy all arguments as column type and not string literals
    sampleDF.select($"ID",myUDFwithTwoArgs($"TEXT",lit("#"))).show(false)
  }

  case class Person(firstName: String, lastName: String, age: Int,
                    weight: Option[Double], jobType: Option[String])
}