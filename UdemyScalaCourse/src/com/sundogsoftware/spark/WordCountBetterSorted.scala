package com.sundogsoftware.spark

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.log4j._

/** Count up how many of each word occurs in a book, using regular expressions and sorting the final results */
object WordCountBetterSorted {
 
  /** Our main function where the action happens */
  def main(args: Array[String]) {
   
    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)
    
     // Create a SparkContext using the local machine
    val sc = new SparkContext("local", "WordCountBetterSorted")   
    
    val path="/Users/lek18/Documents/SparkScala"
    // Read each line of my book into an RDD
    val input = sc.textFile(path+"/book.txt")
    
    // Split using a regular expression that extracts words
    val words = input.flatMap(x => x.split("\\W+"))
    
    
     def filterFunc(x:String) : String ={
    if(x=="the" |  x=="a" | x=="it" |x=="so") " "
    else x
    }
    
    // Normalize everything to lowercase
    val lowercaseWords = words.map(x => x.toLowerCase()).filter(x=>x!="the"|  x=="a" | x=="it" |x=="so")
    
    // Count of the occurrences of each word
    val wordCounts = lowercaseWords.map(x => (x, 1)).reduceByKey( (x,y) => x + y )
    
    // Flip (word, count) tuples to (count, word) and then sort by key (the counts)
    val wordCountsSorted = wordCounts.map( x => (x._2, x._1) ).sortByKey()
    
    // Print the results, flipping the (count, word) results to word: count as we go.
    for (result <- wordCountsSorted) {
      val count = result._1
      val word = result._2
      println(s"$word: $count")
    }
    
  }
  
}

