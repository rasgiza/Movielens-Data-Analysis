

//import packages
spark-shell --packages com.databricks:spark-csv_2.10:1.5.0

import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StringType

/**
 * @author training
 */
object Listofgenres {
  val customSchema = StructType(Array(
    StructField("movieId", StringType, true),
    StructField("title", StringType, true),
    StructField("genres", StringType, true)))
   
  //Loads the CSV file as a Spark DataFrame
  val movie_dataframe = sqlContext.read.format("com.databricks.spark.csv").option("inferSchema","true").option("delimeter",";").schema(customSchema).load("/user/sefa9214/ml-latest/movies.csv")
  
  //filter the genres column
  val filter_dataframe=movie_dataframe.select("genres") 
  
    //Convert the dataframe to RDD
  val genres_Rdd=filter_dataframe.select("genres").rdd

 // filter out the header
    val header = genres_Rdd.first()
    val ratingsRDD_header = genres_Rdd.filter(line=>line != header)
  
  
  //the rdd is in the form of sql dataframe, this convert is  it to string
  val genres_toString = ratingsRDD_header.map(_.toString)

  // Replace pip with commas and replace all brackets
  val genres_pip = genres_toString.map(_.replace("|",",")).map(_.replace("[","")).map(_.replace("]",""))
  
  //Apply the action on the transformed Rdd
  //val genres_array = genres_pip.collect
  
  //generate Rdd by sc.parallelize
  //val New_genresRDD = sc.parallelize(genres_array)
  
  //After the rdd, this create a new row rdd
  val genres_flat=New_genresRDD.flatMap(x=>x.split(","))
  
  // with a little cleaning, "no genres listed can be prune out of the row listed
   val no_genres_listed = genres_flat.map(_.replace("(no genres listed)",""))
   
  //Due to the different rating of movie, multiple genres shows up. Use distinct to get the unique key
  val genres_distinct = no_genres_listed.distinct
  
  //sort the list and filter whitespace
  val final_results = genres_distinct.filter(line=>(line !="")).sorted.foreach(println)
}