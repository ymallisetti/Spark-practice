package deltalake

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.DataFrame
import util.SparkUtil._
import org.apache.spark.sql.SaveMode
import scala.util.Try
import io.delta.tables._

//this is the demo for merge/upsert. If record exist, update
//else create a new record

object DeltaLakeMergeDemo {
  def main(args: Array[String]): Unit = {
    println("Start of DeltaLakeMergeDemo")

    val writeLocation = "D:\\junk\\delta\\deltaupsert"

    val spark = getLocalSparkSession

    //read the sample data
    val pat_raw_df = readDataFrame(spark, "inputs/patient_dim.csv")
    
    //perform some transformation
    val pat_trans_df = pat_raw_df.transform(formatDateColumns)

    //write the data into delta table format
    pat_trans_df.write.format("delta").mode(SaveMode.Overwrite).save(writeLocation)
    
    println("Records before merging")
    val deltaTable = DeltaTable.forPath(writeLocation)
    deltaTable.toDF.show
    
    //read the incremental data and transform the data
    val pat_inc_df = readDataFrame(spark, "inputs/patient_inc.csv")
    val pat_trans_inc_df = pat_inc_df.transform(formatDateColumns)
    
    deltaTable.as("historical")
              .merge(pat_trans_inc_df.as("incremental"), "historical.PatientID = incremental.PatientID")
              .whenMatched()
              .updateExpr(
                  Map(
                     "City" -> "incremental.City",
                     "ZipCode" -> "incremental.ZipCode",
                     "State" -> "incremental.State"
                  )
               )
              .whenNotMatched()
              .insertAll()
              .execute()

    println("Records after merging")
    val deltaTableMerged = DeltaTable.forPath(writeLocation)
    deltaTableMerged.toDF.show
    
    println("End of DeltaLakeMergeDemo")
  }
  
  def formatDateColumns(df: DataFrame): DataFrame = {
    val source_date_format = "MM/dd/yyyy"
    df
      .withColumn("EffFromDate", to_date(col("EffFromDate"), source_date_format))
      .withColumn("EffToDate", to_date(col("EffToDate"), source_date_format))
      .withColumn("BirthDate", to_date(col("BirthDate"), source_date_format))
  }

}