
import org.apache.spark.{ SparkConf, SparkContext }

/**
 * @author training
 */
object PopularMovie {
   def main(args: Array[String]) {
    if (args.length < 3) {
      println("Usage: Popularmovie <input> <output1>")
      System.exit(1)
    }
    //creating a scala Spark Context on the cluster
    val sparkConf = new SparkConf().setAppName("Most Popular Movie")
    val sc = new SparkContext(sparkConf)
    
    //Load the input data
    val ratings_Rdd = sc.textFile("/user/sefa9214/ml-latest/ratings.csv")

    //filter out the header the dataset
    val header = ratings_Rdd.first()
    val ratingsRDD_header = ratings_Rdd.filter(line => line != header)

    //spliting the data by comma and looking at just the column with movie_id and pairing the rdd
    val movie_Id = ratingsRDD_header.map(line => (line.split(",")(1), 1))

    //combine movieid witht the same key
    val movie_count = movie_Id.reduceByKey((x, y) => x + y)

    //load movie input 
    val movie_names=sc.textFile("/user/sefa9214/ml-latest/movies.csv").map(line=>(line.split(",")(0),line.split(",")(1)))
    
    //Removing the first header
     val movie_header = movie_names.first()
     //filter the header
    val Movie_ratings_Rdd = movie_names.filter(line => line != movie_header)
    
    //Perform join using the movieId as primary Key
    val final_join =Movie_ratings_Rdd.join(movie_count)
    
    //Perform sorting
    final_join.sortBy(x=>x._2._2,false).map(x=> x._1+","+x._2._1+","+x._2._2).saveAsTextFile(args(1))
    
    System.exit(0)
   }   
}