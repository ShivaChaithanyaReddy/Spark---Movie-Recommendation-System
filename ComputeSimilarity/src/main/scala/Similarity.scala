
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.mllib.linalg._
import org.apache.spark.mllib.stat.Statistics
import org.apache.spark.rdd.RDD

object Similarity {
  
  
  def main(args: Array[String]): Unit = {
        
    val sc = new SparkContext(new SparkConf().setAppName("Compute Similarity").setMaster("local"))
    
   // var mm : RDD[(String,String)] = sc.emptyRDD;
        
    var mm = sc.parallelize(Seq(("","","","")))
    var forMovies = sc.parallelize(Seq(("","","","")))
    
    
    val data  = sc.textFile("file:/home/cloudera/spark_in_out/LargeOutput/part-00000").map{l => val p = l.split(","); 
    if(p.length == 4)
    (p(0).substring(2,p(0).length()), p(1).substring(0, p(1).length()-1), p(2), p(3).substring(0, p(3).length()-1 )); 
    else
      ("","","","")
      }.filter(_._2.length() > 0)   // filter for excluding movies that contains , in movie name
    
    
    for(a <- 0 to args.length-1) {
      
      if(a%2 == 0)
     args(a) match {
       case "-m" => mm = mm.union(forMovies.sortBy(_._3, false)); forMovies  = findMovie(data, args(a+1)); 
       case "-k" => forMovies = sc.parallelize(numberOfItems(forMovies, args(a+1).toInt))
       case "-l" => forMovies = filterLowerBound(forMovies, args(a+1).toDouble)
       case "-p" => forMovies = filterByRating(forMovies, args(a+1).toInt)
       case _ => println("sorry")
       
     }
    }

val finalData = mm.union(forMovies.sortBy(_._3, false)).filter(_._1.length() > 0)

finalData.repartition(1)saveAsTextFile("/home/cloudera/spark_in_out/similarity_output")  
  finalData.foreach(println)

  }
  
  
  def findMovie(x: RDD[(String, String, String, String)], movieName: String ): RDD[(String, String, String,String)] = {
    x.map{case (m1,m2,cor,number) => if(m1 == movieName)
      (movieName, m2,cor,number);
      else if(m2 == movieName)
      (movieName, m1,cor, number);
      else
        ("","","","");
      }.filter(_._3.length>0)
  }
  
  def numberOfItems(x: RDD[(String, String, String,String)], number: Int): Array[(String,String, String,String)] = {
    x.take(number)
  }
  
  def filterLowerBound(x:RDD[(String,String, String,String)], number: Double): RDD[(String, String, String,String)] =  {
    x.filter( _._2.toDouble > number)
  }
  
    def filterByRating(x:RDD[(String,String, String,String)], number: Int): RDD[(String, String, String,String)] =  {
      x.map(l => l._4).foreach(println)
    x.filter( _._4.toInt >= number)
  }
    
    
}