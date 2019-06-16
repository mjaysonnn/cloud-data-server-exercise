import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

/* Genre Counter */


object GenreCount {
	def main(args: Array[String]) {
		val conf = new SparkConf().setAppName("MovieGenreCounter")
		val sc = new SparkContext(conf)

		val inputDir = args(0)
		val outputDir = args(1)

		val movies = sc.textFile(inputDir) // /dataset/movielens/movies.csv

		val movieInfo = movies.map( movie => movie.split(',') )

		val genres = movieInfo.flatMap( a => a(2).split('|') )
		val genreCount = genres.countByValue.toSeq.sortWith( _._2 > _._2 )

		val outputRdd = sc.parallelize(genreCount)
		outputRdd.saveAsTextFile(outputDir)
	}
}
