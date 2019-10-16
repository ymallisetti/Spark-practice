package deltalake

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.DataFrame
import util.SparkUtil._
import org.apache.spark.sql.SaveMode
import scala.util.Try

case class BollyWoodActorOld(FirstName: String, LastName: String, Phone: Long, Salary: Int)
case class BollyWoodActorNew(FirstName: String, LastName: String, Phone: Long, Salary: Double)

object SparkSchemaOnReadIssue {
  def main(args: Array[String]): Unit = {
    println("Start of SparkSchemaOnReadIssue")

    val spark = getLocalSparkSession
    import spark.implicits._
    val writeLocation = "D:\\junk\\delta\\schemaissue"

    //here salary is INT type
    val actors_old = Seq(BollyWoodActorOld("Akshay", "Kumar", 900000000, 400),
      BollyWoodActorOld("Salman", "Khan", 900000001, 300)).toDF

    //here salary is Double type
    val actors_new = Seq(BollyWoodActorNew("Vicky", "Kaushal", 900000002, 200.05),
      BollyWoodActorNew("Ayushman", "Khurrana", 900000004, 201.44)).toDF

    actors_old.write.mode(SaveMode.Overwrite).option("header", "true").csv(writeLocation)
    actors_new.write.mode(SaveMode.Append).option("header", "true").csv(writeLocation)

    println("Write to the location is succesful")

    val df = spark.read
      .format("csv")
      .option("header", "true") // Use first line of all files as header
      //.option("inferSchema", "true") // Automatically infer data types
      .load(writeLocation)
    println("Showing the appended file data")
    
    //ISSUE - the problem is, the DataType has changed from Int to Double which will be an issue
    df.printSchema
    df.show
    println("End of SparkSchemaOnReadIssue")
  }

}