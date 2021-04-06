package com.sundogsoftware.spark

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.log4j._
//import scala.math.max

/** Find the maximum temperature by weather station for a year */
object MaxTemperatures {
  
  def parseLine(line:String)= {
    val fields = line.split(",")
    val stationID = fields(0)
    val entryType = fields(2)
    val temperature = fields(3).toFloat * 0.1f * (9.0f / 5.0f) + 32.0f
    (stationID, entryType, temperature)
    //the above is a tuple, access it with index starting from 1 to n
  }
    /** Our main function where the action happens */
  def main(args: Array[String]) {
   
    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)
    
    // Create a SparkContext using every core of the local machine
    val sc = new SparkContext("local[*]", "MaxTemperatures")
    val path="/Users/lek18/Documents/SparkScala"
    val lines = sc.textFile(path+"/1800.csv")
    val parsedLines = lines.map(parseLine)
    println(parsedLines.collect())
    
    
    val maxTemps = parsedLines.filter(x => x._2 == "TMAX")
    println(maxTemps.take(1))
    val stationTemps = maxTemps.map(x => (x._1, x._3.toFloat))
    
    def  f(x:Float,y:Float) : Float ={if(x>y)x else y}
    
    val maxTempsByStation = stationTemps.reduceByKey( (x,y) => f(x,y))
    val results = maxTempsByStation.collect()
    
    for (result <- results.sorted) {
       val station = result._1
       val temp = result._2
       val formattedTemp = f"$temp%.2f F"
       println(s"$station max temperature: $formattedTemp") 
    }
      
  }
}