/*package testingdemos

import org.scalatest.FunSuite
import org.apache.spark.SharedSparkContext
import org.scalatest.Assertions._
import com.holdenkarau.spark.testing.DataFrameSuiteBase

class SparkDataframeExample extends FunSuite with DataFrameSuiteBase{
  
  test("simple test") {
    val sqlCtx = sqlContext
    import sqlCtx.implicits._

    val input1 = sc.parallelize(List(1, 2, 3)).toDF
    assertDataFrameEquals(input1, input1) // equal

    val input2 = sc.parallelize(List(4, 5, 6)).toDF
    intercept[org.scalatest.exceptions.TestFailedException] {
        assertDataFrameEquals(input1, input2) // not equal
    }
  }
}*/