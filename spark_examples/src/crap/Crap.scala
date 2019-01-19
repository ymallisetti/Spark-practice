package crap

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Encoders
import scala.reflect.runtime.universe

object Crap {

  case class ExcelRow(workflow_id: String, rule_id: String, source_system: String, table_name: String, column: String, condition: String,
                      join: String, join_key: String, location: String)

  case class Person(name: String, age: Int, personid: Int)
  case class Profile(name: String, personid: Int, profileDescription: String)
  case class Address(addressid: Int, personid: Int, streetaddress: String)

  def main(args: Array[String]): Unit = {
    println("Start of Crap..")

    import org.apache.log4j.{ Level, Logger }
    Logger.getLogger("org").setLevel(Level.ERROR)

    System.setProperty("hadoop.home.dir", "D:\\upload\\dharmik\\winutils")
    val spark = SparkSession.builder().master("local[1]").getOrCreate()

    import spark.implicits._
    val excelDf = spark.createDataFrame(
      ExcelRow("1", "1", "public", "person", "name,age", "age>10", "1", "personid", "local_path")
        :: ExcelRow("1", "2", "public", "profile", "profileDescription", "name='Spark'", "1", "personid", "local_path")
        :: ExcelRow("1", "3", "public", "address", "streetaddress", "name='airoli'", "1", "personid", "local_path") :: Nil)

    //showSparkSqlOutput(spark,personDf,profileDf)

    val excelRowEncoder = Encoders.product[ExcelRow]
    val excelRowDs = excelDf.as[ExcelRow](excelRowEncoder)

    val rowIdDfMap = scala.collection.mutable.Map[String, DataFrame]()
    val rowIdDfList = scala.collection.mutable.ListBuffer[(String, DataFrame)]()
    val rowIdRowExcelMap = scala.collection.mutable.Map[String, ExcelRow]()

    val excelRowList = excelRowDs.collect

    excelRowList.foreach(excelRow => {
      rowIdDfMap += createRuleIdDfPair(excelRow, spark)
      rowIdDfList += createRuleIdDfPair(excelRow, spark)
      rowIdRowExcelMap += (excelRow.rule_id -> excelRow)
    })

    var pdf = rowIdDfMap.get("1").get
    
    //removing DF details for row id =1
    rowIdDfMap.-("1").foreach(pair => {
      pdf=pdf.join(pair._2,"personid")
    })
    
    pdf.show

    println("End of Crap..")
  }


  def createRuleIdDfPair(excelRow: ExcelRow, spark: SparkSession) = {
    (excelRow.rule_id -> resolveDf(excelRow, spark))
  }

  def resolveDf(excelRow: ExcelRow, spark: SparkSession): DataFrame = {
    val tableName = excelRow.table_name
    tableName match {
      case "person"  => createPersonDf(spark)
      case "profile" => createProfileDf(spark)
      case "address" => createAddressDf(spark)
    }
  }

  def showSparkSqlOutput(spark: SparkSession, personDf: DataFrame, profileDf: DataFrame) {
    personDf.createOrReplaceTempView("person")
    profileDf.createOrReplaceTempView("profile")

    val sparkSqlOutput = spark.sql("""SELECT person.name, person.age, profile.profileDescription
                  FROM  person JOIN  profile
                  ON person.personid == profile.personid""")

    sparkSqlOutput.show
  }

  def createProfileDf(spark: SparkSession): DataFrame = {
    spark.createDataFrame(
      Profile("Spark", 2, "SparkSQLMaster") :: Profile("Spark", 5, "SparkGuru") :: Profile("Spark", 9, "DevHunter") :: Nil).as("profile")
  }

  def createPersonDf(spark: SparkSession): DataFrame = {
    spark.createDataFrame(
      Person("Bindu", 20, 2) :: Person("Raphel", 25, 5) :: Person("Ram", 40, 9) :: Nil).as("person")
  }

  def createAddressDf(spark: SparkSession): DataFrame = {
    spark.createDataFrame(
      Address(1, 2, "Airoli") :: Address(2, 5, "Bandra") :: Address(3, 9, "Central") :: Nil).as("address")
  }

  def crap {
    //getting and single query at a time, fetching by workflow id
    //val querydf = excelDf.where($"workflow_id" === "1")

    /*val ruleDf = spark.read
      .option("header", "true")
      .csv("inputs/rule.csv")
      //.withColumn("unitPrice", 'UnitPrice.cast(DoubleType))
      .as[ExcelRow]
	*/
  }
  
  def getJoinedDf(list: List[(String, String)]): String = {

    def dfJoiner(list: List[(String, String)]): String = {
      list match {
        case Nil => " end"
        case x :: tail => {
          x._2 + dfJoiner(tail)
        }
      }
    }
    dfJoiner(list)
  }
}