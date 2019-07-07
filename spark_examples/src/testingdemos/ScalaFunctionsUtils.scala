package testingdemos

object ScalaFunctionsUtils {
  def dumbAdd(a:Int,b:Int) :Int = {
    if(a<0 || b<0)
      throw new IllegalArgumentException
    a+b
  }
}