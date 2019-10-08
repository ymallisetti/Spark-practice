package crap

import util.SparkUtil._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Row

object ComplexAndNestedDataJson {
  def main(args: Array[String]): Unit = {
    println("Start of ComplexAndNestedDataJson example")
    val spark = getLocalSparkSession
    import org.apache.spark.sql.functions.array_contains
    import spark.implicits._
    import org.apache.spark.sql.types._
    
    val rawJson = getRawJsonDF(spark, "user4.json")
    
    rawJson.show(false)
    rawJson.printSchema
    
    //flattening a struct type
    val df1 = rawJson.select($"dc_id", $"source.sensor-igauge.description", $"source.sensor-ipad.description").show
    
    //getting all values from struct of struct type 
    val df2 = rawJson.select($"dc_id", $"source.*").show(false)
    
    val df3 = rawJson.select($"dc_id", explode($"source")).show

    println("End of ComplexAndNestedDataJson example")
  }
  
  def getRawJsonDF(spark: SparkSession, fileName: String): DataFrame = {
    spark
      .read
      //.option("mode", "DROPMALFORMED")
      .option("multiLine", true)
      .json("inputs/jsons/" + fileName)
  }

  def getRawJsonData = Seq("""
                      {
                      "dc_id": "dc-101",
                      "source": {
                          "sensor-igauge": {
                            "id": 10,
                            "ip": "68.28.91.22",
                            "description": "Sensor attached to the container ceilings",
                            "temp":35,
                            "c02_level": 1475,
                            "geo": {"lat":38.00, "long":97.00}                        
                          },
                          "sensor-ipad": {
                            "id": 13,
                            "ip": "67.185.72.1",
                            "description": "Sensor ipad attached to carbon cylinders",
                            "temp": 34,
                            "c02_level": 1370,
                            "geo": {"lat":47.41, "long":-122.00}
                          },
                          "sensor-inest": {
                            "id": 8,
                            "ip": "208.109.163.218",
                            "description": "Sensor attached to the factory ceilings",
                            "temp": 40,
                            "c02_level": 1346,
                            "geo": {"lat":33.61, "long":-111.89}
                          },
                          "sensor-istick": {
                            "id": 5,
                            "ip": "204.116.105.67",
                            "description": "Sensor embedded in exhaust pipes in the ceilings",
                            "temp": 40,
                            "c02_level": 1574,
                            "geo": {"lat":35.93, "long":-85.46}
                          }
                        }
                       }""")
}