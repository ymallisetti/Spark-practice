package crap
import util.SparkUtil._
import org.apache.spark.sql.functions._
import org.apache.avro.ipc.specific.Person
import org.apache.spark.sql.Encoders
import org.apache.spark.sql.types.TimestampType
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.DataFrame

object Crap5 {
  def main(args: Array[String]): Unit = {
    println("Start of Crap5 nested case class demo example")

    val spark = getLocalSparkSession
    import spark.implicits._

    val dataframe = spark.read.json(Seq(getJsonData).toDS)
    dataframe.show(false)
    dataframe.printSchema
    
    var dataframeflattenedalttmp = dataframe.withColumn("temp",explode(array(col("response.*"))))
    var dataframeflattenedalt = dataframeflattenedalttmp
                                      .withColumn("currency",explode(array(col("temp.currency"))))
                                      .withColumn("rate",explode(array(col("temp.rate"))))
                                      .drop("temp","response")
    dataframeflattenedalt.show(false)

    println("End of Crap5 nested case class demo example")
  }

  def getJsonData(): String = {
    """
      { "id" : "595ada836ef4fb4fe47d8c01",
       "response" : { 
       "0" : { "currency" : "JPY", "rate" : 112.27 }, 
       "1" : { "currency" : "AUD", "rate" : 1.30078 },
       "2" : { "currency" : "EUR", "rate" : 0.87544 }, 
       "3" : { "currency" : "GBP", "rate" : 0.76829 },
       "4" : { "currency" : "CNY", "rate" : 6.77907 } 
       }, 
       "ratesDate" : "2017–07–03",
       "createdAt" : "2017–07–04T00:00:03.421Z",
       "updatedAt" : "2017–07–04T00:00:03.421Z" }  
    """
  }

}