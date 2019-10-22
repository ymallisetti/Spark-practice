package deltalake

import java.sql.Date
import java.text._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.DataFrame
import util.SparkUtil._
import org.apache.spark.sql.SaveMode
import io.delta.tables._

case class CustomerUpdate(customerId: Int, address: String, effectiveDate: Date)
case class Customer(customerId: Int, address: String, current: Boolean, effectiveDate: Date, endDate: Date)

object DeltaLakeSCD2Demo {

  implicit def date(str: String): Date = Date.valueOf(str)

  def main(args: Array[String]): Unit = {
    println("Start of DeltaLakeSCD2Demo")

    val spark = getLocalSparkSession
    import spark.implicits._
    val writeLocation = "D:\\junk\\delta\\deltascd2"

    /*Seq(
      Customer(1, "old address for 1", false, null, "2018-02-01"),
      Customer(1, "current address for 1", true, "2018-02-01", null),
      Customer(2, "current address for 2", true, "2018-02-01", null),
      Customer(3, "current address for 3", true, "2018-02-01", null)).toDF().write.format("delta").mode(SaveMode.Overwrite).save(writeLocation)*/

    //updates incremental data
    val newCustomers = Seq(
      CustomerUpdate(1, "new address for 1", "2018-03-03"), //new address TYPE2 update
      CustomerUpdate(3, "current address for 3", "2018-04-04"), // same current address for customer 3 
      CustomerUpdate(4, "new address for 4", "2018-04-04")).toDF() // insert

    //SCD TYPE MERGE PROCESS
    val customersTable: DeltaTable = // table with schema (customerId, address, current, effectiveDate, endDate)
      DeltaTable.forPath(writeLocation)

    val updatesDF = newCustomers // DataFrame with schema (customerId, address, effectiveDate)

    // Rows to INSERT new addresses of existing customers
    val newAddressesToInsert = updatesDF
      .as("updates")
      .join(customersTable.toDF.as("customers"), "customerid")
      .where("customers.current = true AND updates.address <> customers.address")
    newAddressesToInsert.show(false)
    /**
     *  +----------+-----------------+-------------+---------------------+-------+-------------+-------+
        |customerId|address          |effectiveDate|address              |current|effectiveDate|endDate|
        +----------+-----------------+-------------+---------------------+-------+-------------+-------+
        |1         |new address for 1|2018-03-03   |current address for 1|true   |2018-02-01   |null   |
        +----------+-----------------+-------------+---------------------+-------+-------------+-------+
     */
    
    // Stage the update by unioning two sets of rows
    // 1. Rows that will be inserted in the `whenNotMatched` clause
    // 2. Rows that will either UPDATE the current addresses of existing customers or INSERT the new addresses of new customers
    val stagedUpdates = newAddressesToInsert
      .selectExpr("NULL as mergeKey", "updates.*")   // Rows for 1.
      .union(
        updatesDF.selectExpr("customerId as mergeKey", "*")  // Rows for 2.
      )
    stagedUpdates.show(false)
    /**
     *  +--------+----------+---------------------+-------------+
        |mergeKey|customerId|address              |effectiveDate|
        +--------+----------+---------------------+-------------+
        |null    |1         |new address for 1    |2018-03-03   |
        |1       |1         |new address for 1    |2018-03-03   |
        |3       |3         |current address for 3|2018-04-04   |
        |4       |4         |new address for 4    |2018-04-04   |
        +--------+----------+---------------------+-------------+
     */
    
    
    // Apply SCD Type 2 operation using merge
    customersTable
      .as("customers")
      .merge(
        stagedUpdates.as("staged_updates"),
        "customers.customerId = mergeKey")
      .whenMatched("customers.current = true AND customers.address <> staged_updates.address")
      .updateExpr(Map(                                      // Set current to false and endDate to source's effective date.
        "current" -> "false",
        "endDate" -> "staged_updates.effectiveDate"))
      .whenNotMatched()
      .insertExpr(Map(
        "customerid" -> "staged_updates.customerId",
        "address" -> "staged_updates.address",
        "current" -> "true",
        "effectiveDate" -> "staged_updates.effectiveDate",  // Set current to true along with the new address and its effective date.
        "endDate" -> "null"))
      .execute()

    //show the merged output
    DeltaTable.forPath(writeLocation).toDF.show(false)
    /**
     *  +----------+---------------------+-------+-------------+----------+
        |customerId|address              |current|effectiveDate|endDate   |
        +----------+---------------------+-------+-------------+----------+
        |1         |old address for 1    |false  |null         |2018-02-01|
        |1         |current address for 1|false  |2018-02-01   |2018-03-03|
        |3         |current address for 3|true   |2018-02-01   |null      |
        |2         |current address for 2|true   |2018-02-01   |null      |
        |1         |new address for 1    |true   |2018-03-03   |null      |
        |4         |new address for 4    |true   |2018-04-04   |null      |
        +----------+---------------------+-------+-------------+----------+
     */
    println("End of DeltaLakeSCD2Demo")
  }

}