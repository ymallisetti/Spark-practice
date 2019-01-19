package type1type2

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.DataFrame
import util.SparkUtil
import org.apache.spark.sql.expressions.Window
import java.text.SimpleDateFormat
object Type2DriverKuduHbase {
  
  def main(args: Array[String]): Unit = {

    val format = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss")
    val date = format.parse("9999-12-32 00:00:00")
    val sqlDate = new java.sql.Timestamp(date.getTime)
    println("Start of TypeTwo update Demo..")
    val outputPath = "D:\\upload\\spark_data\\crap\\old_data"

    import org.apache.log4j.{ Level, Logger }
    Logger.getLogger("org").setLevel(Level.ERROR) //set the logger level 

    //set the property for hadoop home to work spark on local
    System.setProperty("hadoop.home.dir", "D:\\upload\\dharmik\\winutils")
    val spark = SparkSession.builder().master("local[*]").getOrCreate()

    import spark.implicits._
    val masterDataDF=SparkUtil.readDataFrame(spark, "inputs/patient_dim.csv")
    val patientIncDf=SparkUtil.readDataFrame(spark, "inputs/patient_inc.csv")
    println("Patient DIM historical")
    masterDataDF.show(false)
    //patientIncDf.show(false)
    
    val incrementalDataWithSurrogateKeys = patientIncDf.withColumn("PatientSk", unix_timestamp+monotonically_increasing_id)
                                       .withColumn("EffToDate", lit("9999-12-31"))
                                            
    //rearranging the columns 
    val columnsFromDimPat = masterDataDF.columns
    
    val patientIdsFromIncremental = incrementalDataWithSurrogateKeys.select("PatientID").distinct.collect.map(row => row.get(0).toString)
    val masterDataListFiltered = masterDataDF.filter(col("PatientID").isin(patientIdsFromIncremental: _*))
    
    val unionDf = getDataToUpdateWithChecksum(incrementalDataWithSurrogateKeys, masterDataListFiltered, spark, columnsFromDimPat,patientIdsFromIncremental)
    
    println("Union dataframe from Dim and incremental")
    unionDf.show(false)
    
    val windowSpec = Window.partitionBy("PatientID").orderBy(to_date('EffFromDate, "MM/dd/yyyy").desc)
    val windowedDf = unionDf.withColumn("dummy_date", (lag(to_date('EffFromDate,"MM/dd/yyyy"), 1, sqlDate)).over(windowSpec))
        .withColumn("EffToDate", date_format(date_sub(to_date('dummy_date,"MM/dd/yyyy"), 1), "yyyy-MM-dd hh:mm:ss"))
    
    println("windowed DF cleaned")
    
    val cleanWindowdDf = windowedDf.withColumn("EffToDate", date_format($"EffToDate", "MM/dd/yyyy")).drop($"dummy_date")
    cleanWindowdDf.show(false)
    
    println("End of TypeTwo update Demo..")
  }
  
  def getDataToUpdateWithChecksum(incrementalData: DataFrame, historicalData: DataFrame, spark: SparkSession,
                                  columnsFromDimPat: Array[String],patIds:Array[String]): DataFrame = {
    import spark.implicits._
    val incrementalDataWithHashDF = incrementalData.withColumn("Hash", hash(getColumnNamesToTrack(columnsFromDimPat): _*))

    val masterDataWithHashDF = historicalData.withColumn("Hash", hash(getColumnNamesToTrack(columnsFromDimPat): _*))

    println("patients to update from DIM table")
    val finalPatientDimListToUpdate = masterDataWithHashDF.join(incrementalDataWithHashDF, Seq("Hash"), "leftanti").drop("Hash")
    finalPatientDimListToUpdate.show(false)
    
    val patIdToRemoveFromIncremental = masterDataWithHashDF.as("master")
      .join(incrementalDataWithHashDF.as("inc"), Seq("Hash")).select($"inc.*")
      .select("PatientID")
      .distinct
      .collect.map(row => row.get(0).toString)
      
    println("Unwanted record to remove from incremental :")
    patIdToRemoveFromIncremental.foreach(println)
    println
    val patIdsToFetchFromIncrementalData = patIds.filterNot(patIdToRemoveFromIncremental.contains(_))
    
    val noDupFromInc=incrementalDataWithHashDF
        .filter(col("PatientID").isin(patIdsToFetchFromIncrementalData: _*))
        .select(columnsFromDimPat.head, columnsFromDimPat.tail: _*)

    println("patients to update from INC table")
    noDupFromInc.show(false)
    finalPatientDimListToUpdate.union(noDupFromInc)
  }

  
  def getColumnNamesToTrack(allColumns:Array[String])  = {
    
    var skipColumns = Array("PatientSk","EffToDate")
    
    allColumns.filterNot(skipColumns contains(_)).map(colName=>col(colName))
    
  }


}