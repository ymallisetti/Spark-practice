package examples

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.DataFrame
import util.SparkUtil._

case class PIVObj(A:String,B:String,C:Int)
case class PIVTemprature(date:String,temp:Int)

object PivotDemos {
  def main(args: Array[String]): Unit = {
    println("Start of PivotDemos example")
    val spark = getLocalSparkSession
    
    simlePivotExampleDemos(spark)
    temperatureAverageOverMonthsDemo(spark)
    averageDelayInAirline(spark)
    
    println("End of PivotDemos example")
  }
  
  def averageDelayInAirline(spark:SparkSession) = {
    import spark.implicits._
    val df = readDataFrame(spark,"inputs/flights.csv")
    df.show
    df.groupBy($"carrier", $"origin",$"dest").pivot("hour").avg("arr_delay").show
  }
  
  def temperatureAverageOverMonthsDemo(spark:SparkSession) = {
    import spark.implicits._
    val tempsDfRaw = List( PIVTemprature("2012-02-20",30),PIVTemprature("2012-02-24",31),PIVTemprature("2013-02-20",33),
                      PIVTemprature("2013-06-12",40),PIVTemprature("2012-01-20",30),PIVTemprature("2019-01-25",20),
                      PIVTemprature("2019-01-26",21),PIVTemprature("2019-01-27",22),PIVTemprature("2019-01-28",19)).toDF
    val tempsDf=tempsDfRaw
      .withColumn("year", year($"date"))
      .withColumn("month", month($"date"))
    tempsDf.groupBy($"year").pivot("month").avg("temp").show
    
    /**
     *  +----------+----+----+-----+
        |      date|temp|year|month|
        +----------+----+----+-----+
        |2012-02-20|  30|2012|    2|
        |2012-02-24|  31|2012|    2|
        |2013-02-20|  33|2013|    2|
        |2013-06-12|  40|2013|    6|
        |2012-01-20|  30|2012|    1|
        |2019-01-25|  20|2019|    1|
        |2019-01-26|  21|2019|    1|
        |2019-01-27|  22|2019|    1|
        |2019-01-28|  19|2019|    1|
        +----------+----+----+-----+
        
        +----+----+----+----+
        |year|   1|   2|   6|
        +----+----+----+----+
        |2013|null|33.0|40.0|
        |2019|20.5|null|null|
        |2012|30.0|30.5|null|
        +----+----+----+----+
     */
  }
  
  def simlePivotExampleDemos(spark:SparkSession) = {
    val lst = List( PIVObj("G","X",1),
                    PIVObj("G","Y",2),
                    PIVObj("G","X",3),
                    PIVObj("H","Y",4),
                    PIVObj("H","Z",5))
    import spark.implicits._
    val df=lst.toDF
    
    val tempDf = df.groupBy($"A",$"B").sum("C")
    tempDf.show //this frame is created for demo purpose
    
    val pivotDf = df.groupBy($"A").pivot("B").sum("C")
    pivotDf.show
    
    /**
     * Showing the working of Pivot with the help of interim df
     *  +---+---+------+				
        |  A|  B|sum(C)|
        +---+---+------+       
        |  G|  X|     4|
        |  H|  Z|     5|
        |  H|  Y|     4|
        |  G|  Y|     2|     
        +---+---+------+
        
        +---+----+---+----+
        |  A|   X|  Y|   Z|
        +---+----+---+----+
        |  G|   4|  2|null|
        |  H|null|  4|   5|
        +---+----+---+----+
     */
  }
}