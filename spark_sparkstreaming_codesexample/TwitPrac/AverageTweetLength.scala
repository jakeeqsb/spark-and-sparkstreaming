package TwitPrac


import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.streaming._
import org.apache.spark.streaming.twitter._
import org.apache.spark.streaming.StreamingContext._
import TwitUtil.Utilities._
import java.util.concurrent._
import java.util.concurrent.atomic._

object AverageTweetLength {

  /** Our main function where the action happens */
  def main(args: Array[String]) {

    // Configure Twitter credentials using twitter.txt
    setupTwitter()

    // Set up a Spark streaming context named "AverageTweetLength" that runs locally using
    // all CPU cores and one-second batches of data
    val ssc = new StreamingContext("local[*]", "AverageTweetLength", Seconds(1))

    // Get rid of log spam (should be called after the context is set up)
    setupLogging()

    // Create a DStream from Twitter using our streaming context
    val tweets = TwitterUtils.createStream(ssc, None)

    // Now extract the text of each status update into DStreams using map()
    val statuses = tweets.map(status => status.getText())

    // Map this to tweet character lengths.
    val lengths = statuses.map(status => status.length())

    // As we could have multiple processes adding into these running totals
    // at the same time, we'll just Java's AtomicLong class to make sure
    // these counters are thread-safe.
    //var totalTweets = new AtomicLong(0)
    //var totalChars = new AtomicLong(0)

    var temp = new AtomicLong(0)
    var longestTweet = new AtomicLong(0)
    // In Spark 1.6+, you  might also look into the mapWithState function, which allows
    // you to safely and efficiently keep track of global state with key/value pairs.
    // We'll do that later in the course.

    var max : Long = 0

    lengths.foreachRDD((rdd, time) => {

      var count = rdd.count()
      if (count > 0) {
        /*
        totalTweets.getAndAdd(count)

        totalChars.getAndAdd(rdd.reduce((x,y) => x + y))

        println("Total tweets: " + totalTweets.get() +
          " Total characters: " + totalChars.get() +
          " Average: " + totalChars.get() / totalTweets.get())
        */

        val temp = rdd.reduce(_+_)

        if(max < temp)
          max = temp;

        println( "Current Longest : " + max)

      }
    })

    // Set a checkpoint directory, and kick it all off
    // I could watch this all day!

    ssc.start()
    ssc.awaitTermination()
  }

}
