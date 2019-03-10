package examples

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.DataFrame
import util.SparkUtil._

case class RailCustomer(id: Int, customer: String)
case class RailAgents(id: Int, agent: String)
case class ConfirmTickets(id: Int, seat: String)

object DataFrameJoins {
  def main(args: Array[String]): Unit = {
    println("Start of DataFrameJoins example")

    val spark = getLocalSparkSession
    val custDf = spark.createDataFrame(getAllRailCustomers)
    val ticketsDf = spark.createDataFrame(getAllConfirmSeatDetails)
    
    innerJoinDemos(custDf, ticketsDf,spark)
    
    leftAndRightJoinDemos(custDf, ticketsDf,spark)
    
    left_semi_JoineDemos(custDf, ticketsDf,spark)
    
    left_anti_JoineDemos(custDf, ticketsDf,spark)
    
    println("Start of DataFrameJoins example")
  }
  
  def left_semi_JoineDemos(custDf:DataFrame,ticketsDf:DataFrame,spark:SparkSession) {
    println("Get all Rail customers who have confirmed tickets")
    custDf.join(ticketsDf, Seq("id"), "left_semi" ).show
    /**
     *  +---+--------+
        | id|customer|
        +---+--------+
        |  1| Dharmik|
        |  3|   Sweta|
        |  5|   Rishi|
        +---+--------+
     */
  }
  
  def left_anti_JoineDemos(custDf:DataFrame,ticketsDf:DataFrame,spark:SparkSession) {
    println("Get all Rail customers who does not have any confirmed tickets")
    custDf.join(ticketsDf, Seq("id"), "left_anti" ).show 
    /**
     *  +---+--------+
        | id|customer|
        +---+--------+
        |  2|  Pariya|
        +---+--------+
     */
  }
  
  def leftAndRightJoinDemos(custDf:DataFrame,ticketsDf:DataFrame,spark:SparkSession) {
    custDf.join(ticketsDf, Seq("id"), "left" ).show //all rows from left table even no matchinig from right
    /**
     *  +---+--------+--------+
        | id|customer|    seat|
        +---+--------+--------+
        |  1| Dharmik|   S3/38|
        |  3|   Sweta|   S3/35|
        |  2|  Pariya|    null|
        |  5|   Rishi|RAC/S8-8|
        +---+--------+--------+
     */
    
    custDf.join(ticketsDf, Seq("id"), "right" ).show //all rows from right table even no matching from left
    /**
     *  +---+--------+--------+
        | id|customer|    seat|
        +---+--------+--------+
        |  3|   Sweta|   S3/35|
        |  1| Dharmik|   S3/38|
        |  4|    null|    S1/1|
        |  5|   Rishi|RAC/S8-8|
        | 10|    null|   B1/30|
        +---+--------+--------+
     */
  }
  
  def innerJoinDemos(custDf:DataFrame,ticketsDf:DataFrame,spark:SparkSession) {
    custDf.join(ticketsDf, Seq("id") ).show //default join is inner, only single join column appears
    /**
     *  +---+--------+--------+
        | id|customer|    seat|
        +---+--------+--------+
        |  1| Dharmik|   S3/38|
        |  3|   Sweta|   S3/35|
        |  5|   Rishi|RAC/S8-8|
        +---+--------+--------+
     */
    custDf.join(ticketsDf, custDf("id") === ticketsDf("id") ).show //duplicate join columns appears
    /**
     *  +---+--------+---+--------+
        | id|customer| id|    seat|
        +---+--------+---+--------+
        |  1| Dharmik|  1|   S3/38|
        |  3|   Sweta|  3|   S3/35|
        |  5|   Rishi|  5|RAC/S8-8|
        +---+--------+---+--------+
     */
  }
  
  //S1/1 and B1/30 has no matching id in the customers tables
  def getAllConfirmSeatDetails = 
    List(ConfirmTickets(3,"S3/35"),ConfirmTickets(1,"S3/38"),ConfirmTickets(4,"S1/1"),ConfirmTickets(5,"RAC/S8-8"),
        ConfirmTickets(10,"B1/30"))
  
  //only Pariya does not have any matching rows in confirm tickets tables
  def getAllRailCustomers = 
    List(RailCustomer(1,"Dharmik"),RailCustomer(3,"Sweta"),RailCustomer(2,"Pariya"),RailCustomer(5,"Rishi"))
  
}