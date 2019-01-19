package type1type2

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Encoders
import util.SparkUtil

object TypeOneDriver {

  def main(args: Array[String]): Unit = {

    println("Start of TypeOne update Demo..")
    val outputPath = "D:\\upload\\spark_data\\crap\\old_data"

    import org.apache.log4j.{ Level, Logger }
    Logger.getLogger("org").setLevel(Level.ERROR) //set the logger level 

    //set the property for hadoop home to work spark on local
    System.setProperty("hadoop.home.dir", "D:\\upload\\dharmik\\winutils")
    val spark = SparkSession.builder().master("local[*]").getOrCreate()

    //overwrite specific partitions instead of all, else incremental files
    //will remove all the partitions and generate only those which are
    //available in the updated data. Old data will be lost as insert is Overwrite
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

    import spark.implicits._
    //save employees data and partition by year
    val employeesData = saveEmployeesDataPartitionedByYear(spark)
    //checking the data before updating partitions
    val beforeLocation = employeesData.select("City").where($"Id" === "9").as(Encoders.STRING).collectAsList()
    println("City before the update = " + beforeLocation.get(0))

    //get incremental data
    val incDf = getIncrementalData(spark)
    println("Incremental data, updted ")
    incDf.show(false)

    //get year for which data has been changed
    val yearColsList = incDf.select("CreatedYear").distinct().collect().map(row => row.get(0).toString)

    println("Years for which the data has been changed and those partitions will be updated")
    yearColsList.foreach(print)

    println("")
    //get sales order data for years present in incremental data
    //Since we will be removing complete partition data, we will fetch the data for which we will update
    //minus the data which is present in the incremental, merge the remaining data with inc
    //this will give the complete updated data for partition with all data updated
    val oldEmployeesDataFiltered = employeesData.filter(col("CreatedYear").isin(yearColsList: _*))
    println("Gel all employee data only for years which are present in incremental data")
    oldEmployeesDataFiltered.show(false)

    println("Get all employee data which are not in the incremental data")
    val allEmployeesDataExceptIncremental = oldEmployeesDataFiltered.join(incDf, Seq("Id"), "leftanti")
    allEmployeesDataExceptIncremental.show(false)

    //merge and update the original sales order data partition
    val stagingDf = allEmployeesDataExceptIncremental.union(incDf)
    println("Staging dataframe")
    stagingDf.show(false)

    //write the updated data with dynamic partition property, it will retain all the partitions and will update
    //only those partitions which are in year data
    stagingDf.write.partitionBy("CreatedYear").mode(SaveMode.Overwrite).option("header", true).option("delimiter", ",")
      .csv("D:\\upload\\spark_data\\crap\\old_data")

    //check and verify for the updated data
    val updatedEmployeeDf = SparkUtil.readDataFrame(spark, "D:\\upload\\spark_data\\crap\\old_data")
    val afterLocation = updatedEmployeeDf.select("City").where($"Id" === "9").collect().map(row => row.get(0).toString)
    println("City after the update = " + afterLocation(0))

    println("End of TypeOne update Demo..")
  }

  def getIncrementalData(spark: SparkSession): DataFrame = {
    val employeeDataIncremetalPath = "inputs/employee_inc.csv"
    val incrRaw = SparkUtil.readDataFrame(spark, employeeDataIncremetalPath)

    addYearColumn(spark, incrRaw)
  }

  def saveEmployeesDataPartitionedByYear(spark: SparkSession): DataFrame = {
    val employeeDataHistoricalPath = "inputs/employee_historical.csv"
    val employeeDataRaw = SparkUtil.readDataFrame(spark, employeeDataHistoricalPath)

    val employeeDataWithYear = addYearColumn(spark, employeeDataRaw)

    employeeDataWithYear.write.partitionBy("CreatedYear").mode(SaveMode.Overwrite).option("header", true).option("delimiter", ",")
      .csv("D:\\upload\\spark_data\\crap\\old_data")

    employeeDataWithYear
  }

  def addYearColumn(spark: SparkSession, df: DataFrame): DataFrame = {
    import spark.implicits._

    //convert CreateDate from String --> Date
    val dfFormated = df.withColumn("CreatedDate", to_date($"CreatedDate", "MM/dd/yyyy"))

    //add column in the DataFrame with value of Year from extracted from CreatedDate column
    val dfWithYear = dfFormated.withColumn("CreatedYear", year($"CreatedDate"))
    dfWithYear
  }

}