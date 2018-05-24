case class Person(firstName: String, lastName: String, age: Int,
                    weight: Option[Double], jobType: Option[String])
                    
val peopleDF = spark.createDataFrame(List(
      Person("Justin", "Pahone", 32, None, Some("Programmer")),
      Person("John", "Smoth", 22, Some(170.0), None),
      Person("Jane ", "Daw", 16, None, None),
      Person(" jane", "Smith", 42, Some(120.0), Some("Chemical Engineer")),
      Person("John", "Daw", 25, Some(222.2), Some("Teacher"))))

val sampleDF = spark.createDataFrame(List((1,"This is demo string"),(2,"Yet another demo small"))).toDF("ID","TEXT")

--some functions demos
 peopleDF.groupBy($"firstName").agg(first($"weight")).show //aggregate on non agg columns like weight
 
 val correctedFirstNamePeopleDF = peopleDF.withColumn("firstName",trim(initcap($"firstName"))) //since first name case is diff and having spaces
 
      
