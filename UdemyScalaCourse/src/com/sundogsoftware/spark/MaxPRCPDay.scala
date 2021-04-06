package com.sundogsoftware.spark


import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.log4j._


object MaxPRCPDay {
   def parseLine(line:String)= {
    val fields = line.split(",")
    //val stationID = fields(0)
    val date =fields(1)
    val entryType = fields(2)
    val prcp = fields(3).toFloat
    ( date,entryType, prcp)
  }
  //ourt main function
  def main(args: Array[String]) {
   
    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)
    
    // Create a SparkContext using every core of the local machine
    val sc = new SparkContext("local[*]", "MaxPRCPDay")
    val path="/Users/lek18/Documents/SparkScala"
    val lines = sc.textFile(path+"/1800.csv")
    val parsedLines = lines.map(parseLine)
    val maxPRCP = parsedLines.filter(x => x._2 == "PRCP")
    val stationPRCP = maxPRCP.map(x => (1, x._3.toFloat))
    
    def  f(x:Float,y:Float) : Float ={if(x>y)x else y}
    
    val maxPRCPByStation = stationPRCP.reduceByKey( (x,y) => f(x,y))
    val results = maxPRCPByStation.collect()
    
    
       val MAXprcp = results(0)._2
     val ll= parsedLines.filter(x=>x._3==MAXprcp)  
      val ll1= ll.collect() 
       
       println(ll1(0)) 
    
      
  }
  
}