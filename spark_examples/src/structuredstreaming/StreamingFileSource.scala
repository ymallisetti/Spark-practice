package structuredstreaming
import util.SparkUtil._
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StringType

/**
 * RateStreamSource is a streaming source that generates consecutive numbers with timestamp
 * that can be useful for testing and PoCs.
 */
object StreamingFileSource {
  def main(args: Array[String]): Unit = {
    println("Start of StreamingFileSource example")

    val spark = getLocalSparkSession
    import spark.implicits._

    //mandatory to specify schema while reading
    val schema = StructType(Array(
      StructField("phoneId", StringType, true),
      StructField("action", StringType, true)))

    // Read data continuously from an local file location
    // Option 'basePath' must be a directory
    val streamingInputDF = spark
      .readStream
      .schema(schema)
      .option("maxFilesPerTrigger", 1)
      .json("D:\\junk\\streaming-test")

    // Do operations using the standard DataFrame API and write to console
    val streamingCountsDF = streamingInputDF.groupBy($"phoneId",$"action").count()
    println(streamingCountsDF.isStreaming)

    spark.conf.set("spark.sql.shuffle.partitions", "1") // keep the size of shuffles small
    val consoleDataFrameWriter =
      streamingCountsDF
        .writeStream
        .format("console") 
        .trigger(Trigger.ProcessingTime("2 seconds"))
        .outputMode("complete") // complete = all the counts should be in the table

    val query = consoleDataFrameWriter.start()
    query.awaitTermination()

    println("End of StreamingFileSource example")
  }
}