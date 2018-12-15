
import org.apache.spark.{ SparkConf, SparkContext }

/**
 * @author training
 */
object LatestMovie {
   def main(args: Array[String]) {
    if (args.length < 3) {
      println("Usage: LatestMovie <input> <output1>")
      System.exit(1)
    }
     //creating a scala Spark Context on the cluster
    val sparkConf = new SparkConf().setAppName("Most Popular Movie")
    val sc = new SparkContext(sparkConf)
    
    //load the data
    val movies_rdd=sc.textFile("/user/sefa9214/ml-latest/movies.csv")
    
    //filter out the header the dataset
    val movies_header = movies_rdd.first()
    val moviesRDD_header = movies_rdd.filter(line => line != movies_header)
    
    //all the braces around the year is replace
    val movie_replace = moviesRDD_header.map(line=>line.replace(")","")).map(line=>line.replace(" (",","))
    
    // split the string by commas
     val movie_split = movie_replace.map(line=>line.split(","))
     
     //filter out the movies that containes 2018
     val movie_contains = movie_split.filter(line=>line.contains("2018"))
     
     //sort and print out the movies in 2018
     val results = movie_contains.saveAsTextFile(args(1))
    
   } 
}