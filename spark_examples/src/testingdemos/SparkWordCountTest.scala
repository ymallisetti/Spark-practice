/*package testingdemos

import org.apache.spark.sql.SparkSession
import org.scalatest.BeforeAndAfterAll
import org.scalatest.FunSuite

import util.SparkUtil

class SparkWordCountTest extends FunSuite with SparkSessionTestWrapper with BeforeAndAfterAll
{

  val sqlContext = new SQLContext(sc);
  val spark = sqlContext.sparkSession

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

*/