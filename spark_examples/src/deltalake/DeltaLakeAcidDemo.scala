package deltalake

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.DataFrame
import util.SparkUtil._
import org.apache.spark.sql.SaveMode
import scala.util.Try
import io.delta

object DeltaLakeAcidDemo {
  def main(args: Array[String]): Unit = {
    println("Start of DeltaLakeAcidDemo")

    System.setProperty("spark.home.dir", "D:\\upload\\dharmik\\spark");
    val spark = getLocalSparkSession
    val writeLocation = "D:\\junk\\delta\\overwritedeltalake"
    
    //Execute a normal write operation
    normalWriteOperation(spark, writeLocation)
    
    //Execute error prone write operation
    errorProneWriteOperation(spark, writeLocation)
    
    //read and check that previous write was preserved
    spark.read.parquet(writeLocation).show
    
    println("End of DeltaLakeAcidDemo")
  }

  /**
   * spark.range creates a column name as id
   */
  def normalWriteOperation(spark: SparkSession, path: String) = //format as delta
    spark.range(1,5).repartition(1).write.mode(SaveMode.Overwrite).format("delta").save(path)

  /**
   * spark map method will generate a column name value so there is need to change
   * the column name just to match schema as the demo is on ACID and not on schema
   * This error prone code will not remove the previous parquet file
   * but preserve the previous one.
   * This operation will write till 9 and then fails but data will be written 
   * in the destination path 
   */
  def errorProneWriteOperation(spark: SparkSession, path: String) = {
    import spark.implicits._
    Try(
          spark.range(6,15).repartition(1).map { i =>
            if (i > 9) {
              Thread.sleep(1000)
              throw new RuntimeException("Error occured")
            }
            i
          }.select(col("value").as("id")
        )
       .write.mode(SaveMode.Overwrite).format("delta").save(path))
  }

}