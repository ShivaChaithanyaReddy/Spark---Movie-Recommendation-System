
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

object pairing {
  
  def main(args: Array[String]): Unit = {
    
    val sc = new SparkContext(new SparkConf().setAppName("Pair ALl movies and Ratings").setMaster("local"))
    
val input = sc.textFile("file:/home/cloudera/spark_in_out/combine_output/part-00000");
val in = input.map{  line => val p = line.split(","); (p(0).substring(1,p(0).length()), p(1), p(2).substring(0,p(2).length()-1).toInt )  };
val grouping = in.map{ case (userId, movieName, rating) => (userId, List((movieName,rating)))}.reduceByKey(_++_);
val allPairs = grouping.flatMap{case (user, movieRatings) => (1 until movieRatings.length).flatMap(i => movieRatings.zip(movieRatings drop i))};
val finalPairing = allPairs.map{case ((m1,r1),(m2,r2)) => m1.compareTo(m2) match {
                                                                                  case -1 => ((m1,m2),List((r1,r2)));  
                                                                                  case x if x > 0  => ((m2,m1),List((r2,r1))); 
                                                                                  case _ => ((m1,m2),List((r1,r2)))};
                                                                                  }.reduceByKey(_++_);

val corValue = finalPairing.map{case ((a,b), c) =>  if(c.length>1)  {
  val (r1,r2) = c.unzip;  
                                                     val cobj = new CosineSimilarity();    
                                                     val c1: Double = cobj.cosineSimilarity(r1, r2);
                                                     ((a,b),c1)
                                                     }else {
                                                        (("",""),"1.0".toDouble)
                                                     }
}

 
corValue.filter(_._1._1.length() >0).foreach(println)
corValue.filter(_._1._1.length() >0).saveAsTextFile("/home/cloudera/spark_in_out/pairing_output")  
  }
  
}