package crap

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Encoders
import scala.reflect.runtime.universe
import org.slf4j.LoggerFactory
import org.apache.spark.HashPartitioner
import org.apache.spark.sql.types._
import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.IntegerType
import util.SparkUtil._
import scala.collection.mutable.ListBuffer
import util.SparkUtil._
import util.SparkUtil.Mapping
import util.SparkUtil.PPat
import scala.util.Success
import scala.util.Failure
import java.text.SimpleDateFormat
import scala.util.control._
import scala.util.Try

import scala.collection.mutable.ListBuffer
import examples.DataFrameTransformation
import org.apache.spark.sql.SaveMode
import com.microsoft.azure.storage.blob._

object Crap {

  def main(args: Array[String]): Unit = {
    println("Start of Crap")

    val spark = getLocalSparkSession

    spark.conf.set("fs.azure", "org.apache.hadoop.fs.azure.NativeAzureFileSystem")
    spark.conf.set("fs.azure.account.key.msidwstrg.blob.core.windows.net", "L1Z5euhEc45Z6gPCe7fZpl9ze4cyqSq2ZFCKuuxZ7CJWPkCO0fRR7ggXjwtxW1f4/oZPq2mnI8gljK1gUllLkA==")

    val containerName = "extracts"
    val storageAccount = "msidwstrg"
    val folderAndFileLocation = "master-layer/epic/person"
    val filePath = s"wasbs://$containerName@$storageAccount.blob.core.windows.net/$folderAndFileLocation"

    val df = spark.read.option("header", "true")
      .option("infer", "schema")
      .csv("inputs/employee_inc.csv")
    df.show
      
    println("wrinting to the location : "+filePath)
    df.write
      .mode(SaveMode.Overwrite)
      .parquet(s"$filePath")

    println("End of Crap")
  }

}

