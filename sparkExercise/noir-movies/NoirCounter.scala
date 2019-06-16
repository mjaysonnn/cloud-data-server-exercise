import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

/*
 how you are going to implement Map and Reduce functions to count the number of movies whose genre contains a word “Film-Noir” (movies.csv)
*/
object NoirCount {
	def main(args: Array[String]) {
		val conf = new SparkConf().setAppName("NoirMovieCounter")
		val sc = new SparkContext(conf)

		val inputDir = args(0)
		val outputDir = args(1)

		val movies = sc.textFile(inputDir)

		val movieInfo = movies.map( movie => movie.split(',') )

		val noirCount = movieInfo.filter( movie => movie(2).contains("Film-Noir") ).count

                val outputRdd = sc.parallelize(Seq(noirCount))
                outputRdd.saveAsTextFile(outputDir)
	}
}
