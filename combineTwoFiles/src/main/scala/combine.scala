import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf


object combine {
  def main(args: Array[String]): Unit = {
    
    // create Spark context with Spark configuration
    val sc = new SparkContext(new SparkConf().setAppName("Spark Count").setMaster("local") )

    val file1 = sc.textFile("file:/home/cloudera/spark_in_out/LargeInput/movies.csv")
    val f1 = file1.map{x => val p = x.split(",");
                        (p(0), p(1));
                        }

    val file2 = sc.textFile("file:/home/cloudera/spark_in_out/LargeInput/ratings.csv");
    val f2 = file2.map{x => val p = x.split(",");
                       (p(1), p(0)+"::"+p(2));
                        };
   
    val res = f1.rightOuterJoin(f2).map{case (a, (b:Option[String], c)) => ((c.split("::")(0), (b.getOrElse())+"", c.split("::")(1).toDouble))}
    
    val afterSort = res.sortBy(_._1)
    
    //Pairing code:
    
    val grouping = afterSort.map{ case (userId, movieName, rating) => (userId, List((movieName,rating)))}.reduceByKey(_++_);
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
                                                     ((a,b),c1,r1.length)
                                                     }else {
                                                        (("",""),"1.0".toDouble,0)
                                                     }
}

 
corValue.filter(_._1._1.length() >0).foreach(println)
corValue.filter(_._1._1.length() >0).saveAsTextFile("/home/cloudera/spark_in_out/pairing_output")  

  
  }
}
