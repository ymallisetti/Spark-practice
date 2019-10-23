package structuredstreaming
import util.SparkUtil._
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.streaming.Trigger

object SparkKafkaStructuredStreaming {
  def main(args: Array[String]): Unit = {
    println("Start of SparkKafkaStructuredStreaming example")

    val spark = getLocalSparkSession
    import spark.implicits._
    
    val df = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "kafkawc")
      .load
      
    val consoleDataFrameWriter = 
      df
      .selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
      .writeStream
      .format("console")
      .trigger(Trigger.ProcessingTime("2 seconds"))
      .outputMode(OutputMode.Append())

    val query = consoleDataFrameWriter.start()
    query.awaitTermination()
    
    println("End of SparkKafkaStructuredStreaming example")
  }
}