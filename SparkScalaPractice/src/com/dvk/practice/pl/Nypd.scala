package com.dvk.practice.pl

import org.apache.spark.sql.SparkSession
import org.apache.log4j._

/**
 * This object is used to load a NY pd data, and to analyze
 * the crime pattern and other useful information.
 * Full data can be downloaded from below url
 * https://drive.google.com/open?id=0BzquUESTxeglTTBkSm5PLW1SbVE
 * @author Dharmik
 */
object Nypd {

  def main(args: Array[String]): Unit = {

    val inputFolder = "../files"
    val inputFilePath = "../files/NYPD_huge.csv";
    val outputFolder = ""

    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)

    val sparkSession = SparkSession.builder()
      .appName("NY crime analysis")
      .master("local[*]")
      .getOrCreate()
    val sc = sparkSession.sparkContext
    val sqlContext = sparkSession.sqlContext

    //load the csv directly into dataframes
    /*
     * the infer schema is very slow as spark samples
     * the data in the background to know what the datatype should,
     * instead we should supply the schema and datatype instead.
     */
    val crimes_df = sparkSession.read
      .format("com.databricks.spark.csv")
      .option("header", "true") // Use first line of all files as header
      .option("inferSchema", "true") // Automatically infer data types
      .load(inputFilePath)

    //creating datasets from dataframes
    //import  sparkSession.implicits._ 
    //val crimes_ds = crimes_df.as[schema_crimes]
    
    //you have to change the column names if they have spaes in between, for
    //for all the columns this has to be done
    val columnsRenamed = Seq("OBJECTID","Identifier","Occurrence_Date","Day_of_Week","Occurrence_Month",
        "Occurrence_Day","Occurrence_Year","Occurrence_Hour","CompStat_Month","CompStat_Day",
        "CompStat_Year","Offense","Offense_Classification","Sector","Precinct","Borough",
        "Jurisdiction","XCoordinate","YCoordinate","Location_1")
    val crimes_ren = crimes_df.toDF(columnsRenamed: _*)
    crimes_ren.createOrReplaceTempView("crimes_tb")

    //val records = sqlContext.sql("select Borough from crimes_tb limit 10")
    //records.show()

    ///----------------------removing bad records from data-----------------------------------------
    //start analyzing the data by looking out and filtering
    //out some of the missing values from the crime data
    val offenses = sqlContext.sql("select Offense,count(1) from crimes_tb group by Offense")
    //offenses.show(true) //this shows that there is value 'NA' in the records 

    val years = sqlContext.sql("select Occurrence_Year,count(1) from crimes_tb group by Occurrence_Year")
    //years.show(true) //this shows missing data before year 2006
    
    //so we can filter out records with those years and values as NA
    //-------------------------------------------------------------------------------------------
    
    //now we can check the pattern of the burglary based on year basis
    val burg_years = sqlContext.sql("select Occurrence_Year,count(1) from crimes_tb where Offense ='BURGLARY'"
      +"and Occurrence_Year > 2006 group by Occurrence_Year order by Occurrence_Year")
    burg_years.show()
  }

}
/**/
