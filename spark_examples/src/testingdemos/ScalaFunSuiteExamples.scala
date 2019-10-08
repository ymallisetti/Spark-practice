/*package testingdemos

import org.scalatest.FunSuite
import org.apache.spark.SharedSparkContext
import org.scalatest.Assertions._
import com.holdenkarau.spark.testing.DataFrameSuiteBase

class ScalaFunSuiteExamples extends FunSuite
{
  test("Add function should produce 5 with argument 2 and 3"){
    assert(ScalaFunctionsUtils.dumbAdd(2, 3) == 5)
    assertResult(5)(ScalaFunctionsUtils.dumbAdd(2, 3))
  }
  
  test("Add function when called with negative number should produce expcetion"){
    assertThrows[IllegalArgumentException]{
      ScalaFunctionsUtils.dumbAdd(2, -1)
    }
  }
  
  //testing exceptions
  test("Invoking an empty set element should produce no such element exception"){
    assertThrows[NoSuchElementException]{
      Set.empty.head
    }
  }
}*/