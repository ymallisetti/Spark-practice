package type1type2

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions.Window
import java.text.SimpleDateFormat
import scala.collection.JavaConverters._

import org.apache.spark.sql.functions._
import util.SparkUtil
import org.apache.spark.sql.SaveMode

/**
 * The demo is for TYPE 2 update which is on HDFS. Overwrite is the only way to write data on HDFS.
 * Here the data is written onto HDFS partitioned by year. Once the incremental data arrives,
 * only those year's partitions are fetched for which the data has come in incremental. 
 * The data is then merged and updated with the help of window function and ignoring any duplicate 
 * records. Once the data is union and merged, whole partition for the years are overwritten 
 * on to HDFS. 
 * If we are to use KUDU or Hbase, we can provide the updated DF and then update the records directly
 */
object Type2DriverPartitionByYear {

  def main(args: Array[String]): Unit = {
    println("Start of Crap2")
    import org.apache.log4j.{ Level, Logger }
    Logger.getLogger("org").setLevel(Level.ERROR) //set the logger level 
    System.setProperty("hadoop.home.dir", "D:\\upload\\dharmik\\winutils")

    val spark = SparkSession.builder().master("local[*]").getOrCreate()
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

    val mode = "inc"
    val outputPath = "D:\\upload\\spark_data\\crap\\type2"

    val dataToProcessDf = if ("init".equals(mode)) {
      handleIntialLoad(spark, outputPath)
    } else {
      handleType2Update(spark, outputPath)
    }

    dataToProcessDf.write.partitionBy("year").mode(SaveMode.Overwrite).option("header", true).csv(outputPath)
    println("End of Crap2")
  }

  def handleType2Update(spark: SparkSession, historicalDataPath: String): DataFrame = {
    println("Starting Type2 updates")
    val incrementalDataInputLocation = "inputs/patient_inc.csv"
    val sourceDateFormat = "MM/dd/yyyy"

    import spark.implicits._
    //get the incremental data for patient
    val incrementalDataRaw = SparkUtil.readDataFrame(spark, incrementalDataInputLocation)
      .withColumn("EffFromDate", to_date($"EffFromDate", sourceDateFormat)) //change columns to date
      .withColumn("EffToDate", to_date($"EffToDate", sourceDateFormat)) //change columns to date
      .withColumn("BirthDate", to_date($"BirthDate", sourceDateFormat)) //change columns to date
      .withColumn("year", year($"BirthDate"))

    val distinctYearFromIncremental = incrementalDataRaw.select("year").distinct.collect.map(row => row.get(0).toString)

    //masterDataListFiltered will contain only the patient IDs Data from Master which are in common with Incremental and which is most latest 
    val historicalDataRaw = SparkUtil.readDataFrame(spark, historicalDataPath)
      .filter(col("year").isin(distinctYearFromIncremental: _*)) //filter only patient ids from incremental

    println("Historical data filteres")
    historicalDataRaw.show(false)
    println("Incremental data")
    incrementalDataRaw.show(false)
    val unionDfRaw = getDeltaRecordsByComparingChecksum(incrementalDataRaw, historicalDataRaw, spark)
    println("Union of data")
    unionDfRaw.show(false)

    val format = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss")
    val date = format.parse("9999-12-32 00:00:00")
    val sqlDate = new java.sql.Timestamp(date.getTime)

    val windowSpec = Window.partitionBy("PatientID").orderBy($"EffFromDate".desc)
    println("Applying window operation..")
    val windowedDf = unionDfRaw
      .withColumn("dummy_date", (lag("EffFromDate", 1, sqlDate)).over(windowSpec))
      .withColumn("EffToDate", date_format(date_sub($"dummy_date", 1), "yyyy-MM-dd hh:mm:ss"))

    val cleanWindowdDf = windowedDf
      .withColumn("EffToDate", date_format(col("EffToDate"), "yyyy-MM-dd")) //formating dates,removing hours and seconds
      .withColumn("EffFromDate", date_format(col("EffFromDate"), "yyyy-MM-dd")) //formating dates,removing hours and seconds
      .withColumn("BirthDate", date_format(col("BirthDate"), "yyyy-MM-dd")) //formating dates,removing hours and seconds
      .drop(col("dummy_date"))
    cleanWindowdDf.show(false)
    println("End of Type2 updates")
    cleanWindowdDf
  }

  /**
   * function to compare the incremental data with master data for any changes.
   * It will combine the new data from incremental and data from master layer for updates
   */
  def getDeltaRecordsByComparingChecksum(incrementalData: DataFrame, historicalData: DataFrame, spark: SparkSession): DataFrame = {
    import spark.implicits._
    val incCols = incrementalData.columns
    val incrementalDataWithHashDF = incrementalData
      .withColumn("target_checksum", hash(getColumnNamesToTrack(incCols): _*))
    val historicalDataWithHash = historicalData.withColumn("target_checksum", hash(getColumnNamesToTrack(incCols): _*))

    //removing data from incremental, which is exactly same as of master
    val patIdToRemoveFromIncremental = historicalDataWithHash.as("master")
      .join(incrementalDataWithHashDF.as("inc"), historicalDataWithHash("target_checksum") === incrementalDataWithHashDF("target_checksum")).select($"inc.*")
      .select("PatientID")
      .distinct
      .collect.map(row => row.get(0).toString)

    val allPatIdsFromIncremental = incrementalDataWithHashDF.select("PatientID").distinct.collect.map(row => row.get(0).toString)
    val patIdsToFetchFromIncrementalData = allPatIdsFromIncremental.filterNot(allPatIdsFromIncremental.contains(_))

    println("Pat Id To Remove From Incremental == " + patIdToRemoveFromIncremental.mkString(","))

    val noDupFromInc = incrementalDataWithHashDF
      .filter(col("PatientID").isin(patIdsToFetchFromIncrementalData: _*))
    println("Incremental data after removing duplicate records comparing with historical")
    noDupFromInc.show(false)

    historicalDataWithHash.union(noDupFromInc).drop("target_checksum")
  }

  def handleIntialLoad(spark: SparkSession, outPath: String): DataFrame = {

    println("Starting initial load pipeline")
    val sourceDateFormat = "MM/dd/yyyy"
    val historicalDataInputLocation = "inputs/patient_dim.csv"

    import spark.implicits._
    val initialLoadDf = SparkUtil
      .readDataFrame(spark, historicalDataInputLocation)
      .withColumn("EffFromDate", to_date($"EffFromDate", sourceDateFormat)) //change columns to date
      .withColumn("EffToDate", to_date($"EffToDate", sourceDateFormat)) //change columns to date
      .withColumn("BirthDate", to_date($"BirthDate", sourceDateFormat)) //change columns to date
      .withColumn("year", year($"BirthDate"))

    //initialLoadDf.write.partitionBy("year").mode(SaveMode.Overwrite).option("header", true).csv(outPath)
    println("Initial data written to parquet partition by year.")
    initialLoadDf
  }

  def getColumnNamesToTrack(allColumns: Array[String]) = {
    var skipColumns = Array("EffFromDate", "EffToDate", "BirthDate")
    allColumns.filterNot(skipColumns contains (_)).map(colName => col(colName))
  }

}