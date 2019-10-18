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

object DeltaLakeMergeHistory {
  def main(args: Array[String]): Unit = {
    println("Start of DeltaLakeMergeHistory")

    val writeLocation = "D:\\junk\\delta\\deltaupsert"

    val spark = getLocalSparkSession

    val deltaTable = DeltaTable.forPath(writeLocation)
    deltaTable.history.show(false)
    
    println("Read the data table of version 0")
    spark.read.format("delta").option("versionAsOf", 0).load(writeLocation).show

    println("End of DeltaLakeMergeHistory")
  }

}