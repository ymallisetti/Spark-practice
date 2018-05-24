package com.dvk.splt

import java.io.File
import scala.xml.XML
import scala.xml.Elem
import com.dvk.util.SplitterConstants.FileType

object XmlSplt {
  def main(args: Array[String]): Unit = {
    val inputFolder = args(0)
    val outputFolder = args(1)
    println(outputFolder)
    val inputLocationFile: File = new File(inputFolder)

    for (file <- inputLocationFile.listFiles()) {
      //check if it is not directory
      if (!file.isDirectory()) { //TODO handle for directory
        processFile(file,outputFolder)
      }
    }

    println("Splitting process completed...")
  }

  def processFile(file: File,outputFolder:String) {
    println("Processing file->"+file.getName)
    //load the xml file
    val xmlFile = XML.loadFile(file)
    println("File type ->"+getFileType(xmlFile))
    if(FileType.Wrapper.equals(getFileType(xmlFile)) ){
      WrapperFileHandler.splitIntoSingleFiles(xmlFile,outputFolder)
    }
  }
  
  def getFileType(xmlFile:Elem):FileType.Value = {
    val poElem = xmlFile \ "po" \ "po_1"//checking for child xmls
    if(poElem.size !=0) FileType.Wrapper
    else FileType.WithoutWrapper
  }
}