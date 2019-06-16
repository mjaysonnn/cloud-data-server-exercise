import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

/*
	 Describe how you are going to implement Map and Reduce functions to count the number of unique movie names (movies.csv)
*/


object UniqueMovies {
	def main(args: Array[String]) {
		val conf = new SparkConf().setAppName("UniqueMovieCounter")
		val sc = new SparkContext(conf)

		val inputDir = args(0) // /dataset/movielens/movies.csv
		val outputDir = args(1)

		val movies = sc.textFile(inputDir)

		val movieInfo = movies.map( movie => movie.split(',') )

		val uniqueMovies = movieInfo.map( movie => movie(1) ).distinct

		uniqueMovies.saveAsTextFile(outputDir)
	}
}
