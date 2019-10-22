package structuredstreaming
import util.SparkUtil._
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.streaming.Trigger

/**
 * RateStreamSource is a streaming source that generates consecutive numbers with timestamp 
 * that can be useful for testing and PoCs.
 */
object StreamingFirstExample {
  def main(args: Array[String]): Unit = {
    println("Start of StreamingFirstExample example")

    val spark = getLocalSparkSession
    val ratesDf = spark
      .readStream
      .format("rate") // <-- use RateStreamSource
      .option("rowsPerSecond", 1)
      .load

    val consoleDataFrameWriter = ratesDf.writeStream
      .format("console")
      .trigger(Trigger.ProcessingTime("2 seconds"))
      .outputMode(OutputMode.Append())

    val query = consoleDataFrameWriter.start()
    query.awaitTermination()

    println("End of StreamingFirstExample example")
  }
}