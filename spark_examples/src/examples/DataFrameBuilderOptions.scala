package examples

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Encoders
import util.SparkUtil
import org.apache.spark.sql.types.TimestampType
import org.apache.spark.sql.expressions.Window
import java.text.SimpleDateFormat
import org.apache.spark.sql.types._

case class Employee(Name: String, Age: Int, Designation: String, Salary: Int, ZipCode: Int)
case class TTable(one:String,two:String,three:String,four:String,five:String,six:String,seven:String,eight:String)

object DataFrameBuilderOptions {
  def main(args: Array[String]): Unit = {
    println("Start of DataFrameBuilderOptions")
    val spark = SparkUtil.getLocalSparkSession
    
    createDataFrameFromTestData(spark)

    println("End of DataFrameBuilderOptions")
  }

  def createDataFrameFromTestData(spark: SparkSession) {
    val employeesData = Seq(Employee("Anto", 21, "Software Engineer", 2000, 56798),
      Employee("Jack", 21, "Software Engineer", 2000, 93798),
      Employee("Mack", 30, "Software Engineer", 2000, 28798),
      Employee("Bill", 62, "CEO", 22000, 45798),
      Employee("Joseph", 74, "VP", 12000, 98798),
      Employee("Steven", 45, "Development Lead", 8000, 98798),
      Employee("George", 21, "Sr.Software Engineer", 4000, 98798),
      Employee("Matt", 21, "Sr.Software Engineer", 4000, 98798))
    
    import spark.implicits._ 
    val empDf = employeesData.toDF
    empDf.show
    

  }
  
  def createDataFrameFromPipeSeperatedTxtFile(spark: SparkSession){
    import spark.implicits._
    val dfRaw = spark.sparkContext.textFile("inputs/kudu_POC.txt")
                               .map(_.split('|'))
                               .map(arr=> TTable(arr(0),arr(1),arr(2),arr(3),arr(4),
                                   arr(5),arr(6),arr(7))).toDF
                        .withColumn("five", col("five").cast(TimestampType)) //casting to Decimal type is tricky
                        .withColumn("six", col("six").cast(new DecimalType(30,25)))
  }
  
  

  def createDataFrameFromCsv(spark: SparkSession) {

  }
}