package testingdemos

import org.scalatest.FunSuite
import org.apache.spark.SharedSparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.SQLContext
import org.scalatest.BeforeAndAfterEach
import util.SparkUtil
import org.scalatest.BeforeAndAfterAll

class SparkWordCountTest extends FunSuite with SparkSessionTestWrapper with BeforeAndAfterAll {

  /*val sqlContext = new SQLContext(sc);
  val spark = sqlContext.sparkSession*/

  import spark.implicits._
  test("Word counts should be equal to expected with 2 size") {
    val sourceDF = Seq(
      ("jets"),
      ("barcelona")).toDF("team")
    assert(sourceDF.count === 2)
  }
  
  test("Word counts should be equal to expected with 3 size") {
    val sourceDF = Seq(
      ("jets"),
      ("barcelona"),
      ("liverpool")).toDF("team")
    assert(sourceDF.count === 3)
  }

  override def afterAll() {
    spark.stop()
  }

}

trait SparkSessionTestWrapper {
  lazy val spark: SparkSession = SparkUtil.getLocalSparkSession
}

