/*package testingdemos

import org.scalatest.{ Matchers, WordSpec }

class ScalaMatchersExample extends WordSpec with Matchers {
  "should check the class of an object" in {
    case class Employee(name: String)
    val employee = Employee("Jane")
    employee shouldBe a[Employee]
  }

  "checking the values of a custom object" in {
    case class Book(title: String, author: String)
    val book = Book("SomeBook", "Jane")
    book should have(
      'title("SomeBook"),
      'author("Jane"))
  }
}*/