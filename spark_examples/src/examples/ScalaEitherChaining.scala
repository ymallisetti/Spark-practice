package examples

object ScalaEitherChaining {
  def main(args: Array[String]): Unit = {
    println("Executing method chaining")
    val res = 
      op1("A").right
      .flatMap(x => op2("B")).right
      .flatMap(x => op3("C"))

    res match {
      case Left(msg)     => println(msg)
      case Right(source) => println(source)
    }
  }

  def op1(arg: String): Either[String, String] = {
    arg match {
      case "A" => Right("A")
      case _   => Left("Failure in op1")
    }
  }

  def op2(arg: String): Either[String, String] = {
    arg match {
      case "B" => Right("B")
      case _   => Left("Failure in op2")
    }
  }
  
  def op3(arg: String): Either[String, String] = {
    arg match {
      case "C" => Right("Success")
      case _   => Left("Failure in op3")
    }
  }
}