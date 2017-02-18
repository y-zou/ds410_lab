package demo

import java.io.PrintWriter
import java.io.File
import java.util.Arrays
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import scala.collection.JavaConversions
import scala.io.Source


object Demo {

	// Application Specific Variables
	private final val SPARK_MASTER = "yarn-client"
	private final val APPLICATION_NAME = "demo"

	// HDFS Configuration Files
	private final val CORE_SITE_CONFIG_PATH = new Path("/usr/hdp/current/hadoop-client/conf/core-site.xml")
	private final val HDFS_SITE_CONFIG_PATH = new Path("/usr/hdp/current/hadoop-client/conf/hdfs-site.xml")

	def main(args: Array[String]): Unit = {

		// Configure SparkContext
		val conf = new SparkConf()
			.setMaster(SPARK_MASTER)
			.setAppName(APPLICATION_NAME)
		val sc = new SparkContext(conf)

		// Configure HDFS
		val configuration = new Configuration();
		configuration.addResource(CORE_SITE_CONFIG_PATH);
		configuration.addResource(HDFS_SITE_CONFIG_PATH);

		//-------------------------------------------------------------------//
		//------------- Your code should starts from here. ------------------//

		//-------------------------------------------------------------------//

		// Print Usage Information
		System.out.println("\n----------------------------------------------------------------\n")
		System.out.println("Usage: spark-submit [spark options] demo.jar [exhibit]")
		System.out.println(" Exhibit \'hw\': HelloWorld")
		System.out.println(" Exhibit \'tweet\': Top Tweeters")
		System.out.println("\n----------------------------------------------------------------\n");

		// Exhibit: HelloWorld
		if(args(0) == "hw") {
			System.out.println("Running exhibit: HelloWorld")
			System.out.println("Hello, World!")
		}

		// Exhibit: Top Hashtags
		if(args(0) == "tweeter") {
			System.out.println("Running exhibit: Twitter Data")
			val lines = sc.textFile("hdfs:/ds410/tweets/nyc-twitter-data-2013.csv")
			val users = lines.map(line => line.split(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)")).map(cs => (if (cs.size < 7) ""; else cs(2)))
			val sortedCounts = users.map(user => (user, 1)).reduceByKey(_ + _).sortBy(_._2, false)
			val top10 = sortedCounts.take(10)
			val writer = new PrintWriter(new File("output.txt"))
			top10.foreach(x => writer.write(x._1 + "\t" + x._2 + "\n"))
			writer.close()
		}

		if(args(0) == "hashtag") {
			System.out.println("Running exhibit: Hashtag count")
			val lines = sc.textFile("hdfs:/ds410/tweets/nyc-twitter-data-2013.csv")
			val texts = lines.map(line => line.split(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)")).map(cs => (if (cs.size < 7) ""; else cs(6)))
			val words = texts.flatMap(_.split("[ .,?!:\"]"))
			// fill in your code here
			// ...
			val hashtags = words.filter(s => s(0)=='#')
			val hashtagCounts = hashtags.map(hashtag  => (hashtag, 1)).reduceByKey(_ + _).sortBy(_._2, false)
			val top100 = hashtagCounts.take(100)

			// output result into file, uncomment it after you finish the code
			 val writer = new PrintWriter(new File("output.txt"))
			 top100.foreach(x => writer.write(x._1 + "\t" + x._2 + "\n"))
			 writer.close()
		}
		//-------------------------------------------------------------------//
		//------------------- Your code should end here. --------------------//
		//-------------------------------------------------------------------//
	}
}
