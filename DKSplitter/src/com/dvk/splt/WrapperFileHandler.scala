package com.dvk.splt

import scala.xml.Elem
import com.dvk.util.SplitterConstants._
import scala.xml.XML

object WrapperFileHandler {
  
  def splitIntoSingleFiles(xmlFileElem : Elem,outputFolder:String){
    //get any children after po and filter if it starts with po(po_1, po_2)
    val allChildOrders = (xmlFileElem \"po" \ "_").filter(node => node.label.startsWith("po"))
    for(childOrder <- allChildOrders){
      val singlePO = (childOrder \"purchaseOrder")(0)
      //extract child name from node.label and get the index by splitting
      var purchaseOrderIdx = childOrder.label.split(ChildPurchaseOrderDelemeter)
      XML.save(outputFolder+childOrder.label, singlePO, "UTF-8", true, null)
    }
  }
}