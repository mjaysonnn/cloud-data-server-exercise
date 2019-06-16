import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

/*
 how you are going to implement Map and Reduce functions to list the tag that are cited the most
*/

object TagCount {
	def main(args: Array[String]) {
		val conf = new SparkConf().setAppName("TagCounter")
		val sc = new SparkContext(conf)

		val inputDir = args(0) // /dataset/movielens/tags.csv
		val outputDir = args(1)

		val tags = sc.textFile(inputDir)

		val tagInfo = tags.map( tag => tag.split(',') )

		val onlyTags = tagInfo.map( tag => tag(2) )
		
		val tagCount = onlyTags.countByValue

		val tagRank = tagCount.toSeq.sortWith(_._2 > _._2)

                val outputRdd = sc.parallelize(tagRank)
                outputRdd.saveAsTextFile(outputDir)
	}
}
