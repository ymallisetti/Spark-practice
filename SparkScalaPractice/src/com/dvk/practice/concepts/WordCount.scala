package com.dvk.practice.concepts

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.log4j._

/**
 * this class simply counts the words from a text file
 */
object WordCount {
  
    /** Our main function where the action happens */
  def main(args: Array[String]) {
   
    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)
        
    // Create a SparkContext using every core of the local machine
    val sc = new SparkContext("local[*]", "Words Count")
  
    // Load each line of the source data into an RDD
    val lines = sc.textFile("../files/book.txt")
    
    //split into words separated by spae charater
    val words = lines.flatMap(line=>line.split(" "))
    
    //make tuple of each words and count 1
    val wordCountTuple = words.map(word=>(word,1))
    
    //reduce each word count
    //val wordCountRdd =wordCountTuple.reduceByKey((x,y) =>x+y)
    
    //or simple we can do in short
    val wordCountMap = words.countByValue() 
    //wordCountMap.foreach(println)
    //wordCountRdd.foreach(println) //#### PART 1 #######
    //------------simple word count finishes--------------//
    /*
     * but we need to address some issues like, small and capital words are
     * different, coma with words are taken different, and other special charaters
     * needs to be removed so that the logic could return unique words count
     * example row and row, are same but taken different
     * example SOMEONE and someone are taken different
     */
    
    //with regex we will extract only words and not punctuation
    val wordsClean = lines.flatMap(line => line.split("\\W+"))
    
    val lowerCaseWords = wordsClean.map(word => word.toLowerCase())
    
    val wordsCountClean = lowerCaseWords.countByValue()
    
    //wordsCountClean.foreach(println) //#### PART 2 #######
    
    val wordCount = lowerCaseWords.map(word => (word,1))
                                  .reduceByKey((x,y) =>x+y)
    
    //val wordsCountSorted = wordCount.sortBy(_._2) //----> doest work ????? 
    
    val wordsCountSorted = wordCount
                                .map(item => item.swap)
                                .sortByKey(false, 1) //sort in descending order
                                .map(item =>item.swap)
    wordsCountSorted.foreach(println)
  }//main ends
  
}