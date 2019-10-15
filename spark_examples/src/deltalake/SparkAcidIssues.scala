package deltalake

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.DataFrame
import util.SparkUtil._
import org.apache.spark.sql.SaveMode
import scala.util.Try

object SparkAcidIssues {
  def main(args: Array[String]): Unit = {
    println("Start of SparkAcidIssues")

    val spark = getLocalSparkSession
    val writeLocation = "D:\\junk\\delta\\overwrite"

    //Execute a normal write operation
    normalWriteOperation(spark, writeLocation)
    
    //1) ATOMIC and CONSISTENCY(data is always in valid state) OPERATION FAILS
    //error prone write will remove the previously written files
    //but since the job fails, it will not write the new data also
    //This is not an ATOMIC operation for mode Overwrite, but for 
    //mode Append it will be an atomic
    errorProneWriteOperation(spark, writeLocation)
    
    //2) Spark does not offer isolation levels for read and writes
    //3) Durability is supported in HDFS, S3, but since the spark 
    //write operation is faulty, it does not support durability also

    println("End of SparkAcidIssues")
  }

  def normalWriteOperation(spark: SparkSession, path: String) =
    spark.range(100).repartition(1).write.mode(SaveMode.Overwrite).csv(path)

  def errorProneWriteOperation(spark: SparkSession, path: String) = {
    import spark.implicits._
    Try(
      spark.range(100).repartition(1).map { i =>
        if (i > 50) {
          Thread.sleep(1000)
          throw new RuntimeException("Error occured")
        }
        i
      }.write.mode(SaveMode.Overwrite).csv(path))
  }

}