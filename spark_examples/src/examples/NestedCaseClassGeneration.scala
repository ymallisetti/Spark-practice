package examples

import util.SparkUtil._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.Encoders

case class Pat(pat_id:String, firstname:String, lastName:String, encounters:List[Enc])
case class Enc(enc_id:String, encDate:String, pat_id:String,diagnosis:List[Diag])
case class Diag(diag_id:String, diagType:String, docName:String)

case class Enc_raw(enc_id:String, encDate:String,pat_id:String)
case class Pat_raw(pat_id:String, firstname:String, lastName:String)
case class Diag_raw(diag_id:String, diagType:String, docName:String,enc_id:String)

object NestedCaseClassGeneration {
  
  def main(args: Array[String]): Unit = {
    println("Start of Crap5 nested case class demo example")
    
    val spark = getLocalSparkSession
    
    val PatientRawDf = spark.createDataFrame(getAllRawPatients)
    val EncountersRawDf = spark.createDataFrame(getAllRawEncouners)
    val DiagnosisRawDf = spark.createDataFrame(getAllRawDiagnosis)

    import spark.implicits._
    val encDiagJoinedDf = EncountersRawDf
                              .join(DiagnosisRawDf,Seq("enc_id"),"left")
                              .groupBy("enc_id","encDate","pat_id") //need to provide all the columns in 'Enc' schema
                              .agg(collect_list(struct(DiagnosisRawDf.columns.map(col):_*)).alias("diagnosis") ).as[Enc]
    
    val encDiagJoinedDfNew= EncountersRawDf
                               .join(DiagnosisRawDf
                                         .groupBy("enc_id")
                                         .agg((collect_list(struct(DiagnosisRawDf.columns.map(col):_*))).alias("diagnosis")), "enc_id"
                                     ).as[Enc]  //this method need not declare all encounter columns in the group by clause
    
    encDiagJoinedDfNew.show(false)
    val patEncJoinedDf = PatientRawDf
                              .join(encDiagJoinedDf,Seq("pat_id"),"left")
                              .groupBy("pat_id","firstname","lastName")
                              .agg(collect_list(struct(encDiagJoinedDf.columns.map(col):_*)).alias("encounters") ).as[Pat] 
    
    patEncJoinedDf.show(false)
    
    patEncJoinedDf.write.json("C:\\Users\\dharmikk\\Downloads\\one.json")
    
    println("End of Crap5 nested case class demo example")
  }
  
  
  
  
  def getAllRawPatients = {
    List(Pat_raw("1","Gunjan","Bafna"),
        Pat_raw("2","Tavish","Bhagat"))
  }
  
  def getAllRawEncouners = {
    List(Enc_raw("1","1/1/2014","1"),
        Enc_raw("2","1/5/2019","1"),
        Enc_raw("3","1/12/2018","2"),
        Enc_raw("4","1/1/2015","3"))
  }
  
  def getAllRawDiagnosis = {
    List(Diag_raw("1","Blood Test","SaifiDoc2","2"),
        Diag_raw("2","Mallaria Test","Doctor1","4"),
        Diag_raw("3","Nose Surgery","SaifiDoc1","1"),
        Diag_raw("4","Platlet Test","Fortis2","3"),
        Diag_raw("5","Admitted","Fortis1","3"),
        Diag_raw("6","Diabeties Test","SaifiDoc3","2"))
  }

}