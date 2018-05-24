/**
 * THIS CODE AND INFORMATION ARE PROVIDED "AS IS" WITHOUT WARRANTY OF ANY
 * KIND, EITHER EXPRESSED OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND/OR FITNESS FOR A PARTICULAR PURPOSE.
 * SEE THE COPYRIGHT FILE=' LICENSE.txt' DISTRIBUTED WITH THIS WORK FOR ADDITIONAL
 * INFORMATION REGARDING COPYRIGHT OWNERSHIP
 */
package com.dk.parser

import org.apache.spark.sql.Row


object DummyParser{
  
  def parseFiles() : (List[(String, List[Row])], List[Row]) = {
    
    val assessment1 = Row("1","assessment text","AAERC","22386","AssEntity","AssEntity")
    val assessment2 = Row("2","assessment text2","AAERC","2238677","AssEntity","AssEntity")
    val assessment3 = Row("3","assessment text3","","2238677","AssEntity","AssEntity")
   
    val vital = Row("1","vital text","VVTAL","242","VitalEntity","VitalEntity")
    val assessmentList = List(assessment1)
    
    var assLstComplete = assessment2 ::assessment3:: assessmentList
    var vitalLst = List(vital)
    
    val assTuple = ("AssEntity",assLstComplete)//key and value
    val vitalTuple = ("VitalEntity",vitalLst)//key and value
    var parseList = vitalTuple :: List(assTuple) //parser list
    
    val aurdRow1 = Row("Base File Name1","Practice id1")
    val aurdRow2 = Row("Base File Name2","Practice id2")
    
    var auditLst = aurdRow1 :: List(aurdRow2)
    
    (parseList,auditLst)
  }
  
}

