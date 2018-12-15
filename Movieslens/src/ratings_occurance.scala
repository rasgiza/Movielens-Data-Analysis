import org.apache.spark.{ SparkConf, SparkContext }

/**
 * @author training
 */
object ratings_occurance {
  def main(args: Array[String]) {
//    if (args.length < 3) {
//      println("Usage: popularmovie <input> <output1> <numOutputFiles>")
//      System.exit(1)
//    }
    //creating a scala Spark Context on the cluster
    val sparkConf = new SparkConf().setAppName("Most Rated Movie")
    val sc = new SparkContext(sparkConf)

    //Reading the input data
    val ratingRdd = sc.textFile("/user/sefa9214/ml-latest/ratings.csv")

    //filter out the header the dataset
    val header = ratingRdd.first()
    val ratingRDD_header = ratingRdd.filter(line => line != header)

    //Split data by comma and filter out the ratings column
    val rating_split = ratingRDD_header.map(line => line.split(",")(2))
    
       //tuple of the unique value and the count
    val ratings_count = rating_split.countByValue()
    
    //sort the rating count in order and save
    val rating_sort = ratings_count.toSeq.sortBy(_._1).foreach(println)
    
    System.exit(0)
  }
}
