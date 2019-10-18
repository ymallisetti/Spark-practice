package deltalake

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.DataFrame
import util.SparkUtil._
import org.apache.spark.sql.SaveMode
import scala.util.Try
import io.delta.tables._

object DeltaLakeTableDemo {
  def main(args: Array[String]): Unit = {
    println("Start of DeltaLakeTableDemo")

    val writeLocation = "D:\\junk\\delta\\deltatable"

    val spark = getLocalSparkSession

    //read the sample data
    val pat_raw_df = readDataFrame(spark, "inputs/patient_dim.csv")
    
    //perform some transformation
    val pat_trans_df = pat_raw_df.transform(formatDateColumns)

    //write the data into delta table format
    pat_trans_df.write.format("delta").mode(SaveMode.Overwrite).save(writeLocation)
    
    //read the parquet files in the form of DeltaTable
    //can be read as simple parquet and in format delta also
    val deltaTable = DeltaTable.forPath(writeLocation)
    deltaTable.toDF.show
    
    //update/delete some data in the datatable
    deltaTable.update(
      condition = expr("PatientID == 101"),
      set = Map("FirstName" -> lit("RAKA")))
      
    //different syntax of update
    deltaTable.updateExpr("PatientID == '101'", Map("FirstName" -> "RAKA"))
      
    //deltaTable value is AUTO REFRESHED, no need to read it again
    deltaTable.toDF.show
    
    println("End of DeltaLakeTableDemo")
  }

  def formatDateColumns(df: DataFrame): DataFrame = {
    val source_date_format = "MM/dd/yyyy"
    df
      .withColumn("EffFromDate", to_date(col("EffFromDate"), source_date_format))
      .withColumn("EffToDate", to_date(col("EffToDate"), source_date_format))
      .withColumn("BirthDate", to_date(col("BirthDate"), source_date_format))
  }

}