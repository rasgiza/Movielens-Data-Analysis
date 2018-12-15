import org.apache.spark.{ SparkConf, SparkContext }

/**
 * @author training
 */
object MovieRating {
  def main(args: Array[String]) {
    if (args.length < 3) {
      println("Usage: popularmovie <input> <output1>")
      System.exit(1)
    }
    //creating a scala Spark Context on the cluster
    val sparkConf = new SparkConf().setAppName("Most Rated Movie")
    val sc = new SparkContext(sparkConf)

    //Load the input data
    val ratings_Rdd = sc.textFile("/user/sefa9214/ml-latest/ratings.csv")

    //filter out the header the dataset
    val header = ratings_Rdd.first()
    val ratingsRDD_header = ratings_Rdd.filter(line => line != header)

    //spliting the data by comma and looking at just the column with movie_id and pairing the rdd
    val movie_Id = ratingsRDD_header.map(line => (line.split(",")(1).toInt, 1))

    //combine movieid witht the same key
    val movie_count = movie_Id.reduceByKey((x, y) => x + y)

    //swap count and movieId and sort
    val fin_result = movie_count.map(line=>line.swap).sortByKey(false).saveAsTextFile("/home/training/output2")
    System.exit(0)
  }
}