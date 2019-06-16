import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

/* 
 how you are going to implement Map and Reduce functions to calculate the average rating of each movie (ratings.csv in the movie lens dataset)
*/

object AvrRating {
	def main(args: Array[String]) {
		val conf = new SparkConf().setAppName("AverageRatingEachMovie")
		val sc = new SparkContext(conf)

		val inputDir = args(0)
		val outputDir = args(1)

		val ratings = sc.textFile(inputDir) // /dataset/movielens/ratings.csv

		val ratingInfo = ratings.map( rating => rating.split(',') )

		val movieRatings = ratingInfo.map( mi => (mi(1).toInt, mi(2).toFloat))

		val mapped = movieRatings.mapValues( v => (v, 1) )

		val summed = mapped.reduceByKey{ case ( (sumL, countL), (sumR, countR) ) => (sumL+sumR, countL+countR) }

		val avrRatings = summed.mapValues{ case (sum, count) => sum / count }

		avrRatings.saveAsTextFile(outputDir)
	}
}
