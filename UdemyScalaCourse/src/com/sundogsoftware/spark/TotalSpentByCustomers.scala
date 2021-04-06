

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.log4j._


object TotalSpentByCustomers {

  
  //function to read the file
    def parseLine(line:String)={
      val fields=line.split(",")
      val customerID=fields(0)
      val itemID=fields(1)
      val cost=fields(2).toFloat
      (customerID,itemID ,cost)
    }
    
    def main(args:Array[String]) {
      // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)
    
    // Create a SparkContext using every core of the local machine
    val sc = new SparkContext("local[*]", "MaxTemperatures")
    val path="/Users/lek18/Documents/SparkScala"
    val lines = sc.textFile(path+"/customer-orders.csv")
    val parsedLines = lines.map(parseLine)
    
    ///to print lines
    //parsedLines.foreach(println)
    
    //group by customers
    val customerCost=parsedLines.map(x=>(x._1,x._3))
    
    //find the total price for each customer by using the reduceByKey
     val customerTotalCost=customerCost.reduceByKey((x,y)=>x+y)
     val flipped=customerTotalCost.map(x=>(x._2,x._1)).sortByKey()
     val results=flipped.collect()
    results.foreach(println)
    }
}