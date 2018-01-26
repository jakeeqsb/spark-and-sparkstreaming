package TwitPrac


import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.streaming._
import org.apache.spark.streaming.twitter._
import org.apache.spark.streaming.StreamingContext._
import TwitUtil.Utilities._

object SaveTweets {

  def main(args: Array[String]): Unit =
  {
    //Configure Twitter credentials using twitter.txt
    setupTwitter()

    // Set up a Spark streaming context named "SaveTweets" that runs locally using
    // all CPU cores and one-second batches of data
    val ssc = new StreamingContext("local[*]", "SaveTweets", Seconds(1))

    // Get rid of log spam ()
    setupLogging()

    // Create a DStream from Twitter using our streaming context
    val tweets = TwitterUtils.createStream(ssc, None)

    // Now extract the text of each status update into RDD's using map()
    val statuses = tweets.map(status => status.getText())

    var totalTweeets:Long = 0





    statuses.foreachRDD( (rdd, time) => {
      if(rdd.count() > 0)
      {
        // Combine each partition's results into a single RDD;
        val repartitionedRDD = rdd.repartition(1).cache()

        repartitionedRDD.saveAsTextFile("Tweets_" + time.milliseconds.toString )

        // Stop one we've collected 1000 tweets
        totalTweeets += repartitionedRDD.count()
        println("Tweet count: " + totalTweeets)

        if(totalTweeets > 1000)
        {
          System.exit(0)
        }

      }
    })


    ssc.start()
    ssc.awaitTermination()
  }
}
