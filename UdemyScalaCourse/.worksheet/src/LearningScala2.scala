object LearningScala2 {;import org.scalaide.worksheet.runtime.library.WorksheetSupport._; def main(args: Array[String])=$execute{;$skip(141); 
  // Flow control
  
  // If / else syntax
  if (1 > 3) println("Impossible!") else println("The world makes sense.");$skip(96); 
  
  if (1 > 3) {
  	println("Impossible!")
  } else {
  	println("The world makes sense.")
  };$skip(68); 
  
  // Matching - like switch in other languages:
  val number = 3;System.out.println("""number  : Int = """ + $show(number ));$skip(146); 
  number match {
  	case 1 => println("One")
  	case 2 => println("Two")
  	case 3 => println("Three")
  	case _ => println("Something else")
 	};$skip(87); 
 	
 	// For loops
 	for (x <- 1 to 4) {
 		val squared = x * x
 		println(squared)
 	};$skip(81); 
                                                  
  // While loops
  var x = 10;System.out.println("""x  : Int = """ + $show(x ));$skip(47); 
  while (x >= 0) {
  	println(x)
  	x -= 1
  };$skip(59); 
                                                  
  x = 0;$skip(42); 
  do { println(x); x+=1 } while (x <= 10);$skip(154); val res$0 = 
                                                  
   // Expressions
   // "Returns" the final value in a block automatically
   
   {val x = 10; x + 20};System.out.println("""res0: Int = """ + $show(res$0));$skip(82); 
                                                
	 println({val x = 10; x + 20});$skip(259); 
	 
	 // EXERCISE
	 // Write some code that prints out the first 10 values of the Fibonacci sequence.
	 // This is the sequence where every number is the sum of the two numbers before it.
	 // So, the result should be 0, 1, 1, 2, 3, 5, 8, 13, 21, 34
	var x1=0;System.out.println("""x1  : Int = """ + $show(x1 ));$skip(10); 
	var x2=1;System.out.println("""x2  : Int = """ + $show(x2 ));$skip(94); 
	for (i<-1 to 9){
	if(i==1){println(0)}
 var x3=x1+x2
   println(x2)
   
   x1=x2
   x2=x3
	};$skip(99); 
   def fib(x:Int): Int={
  if(x==1) 0
  else if (x==2) 1
  else {
    fib(x-1)+fib(x-2)
  }
 
   };System.out.println("""fib: (x: Int)Int""");$skip(44); 
   for ( i <- 1  to 10){
   println(fib(i))}
   }
                                                
	   
}
