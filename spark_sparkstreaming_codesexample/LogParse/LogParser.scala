package LogParse

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.storage.StorageLevel

import java.util.regex.Pattern
import java.util.regex.Matcher

import TwitUtil.Utilities._

object LogParser {


  def main(args:Array[String]): Unit = {

    val ssc = new StreamingContext("local[*]", "LogParser", Seconds(1))

    setupLogging()


    // Construct a regular expression (regex) to extract fields from raw Apache log lines
    val pattern = apacheLogPattern()

    // Create a socket stream to read log data published via netcat on port 9999 locally
    val lines = ssc.socketTextStream("127.0.0.1", 9999, StorageLevel.MEMORY_AND_DISK_SER)

    // Extract the request field from each log line
    //val requests = lines.map(x => {val matcher:Matcher = pattern.matcher(x); if (matcher.matches()) matcher.group(5)})
    val requests = lines.map(x => {
      val matcher : Matcher = pattern.matcher(x);
      if (matcher.matches())
        matcher.group(9)
    })
    // Extract the URL from the request
    //val urls = requests.map(x => {val arr = x.toString().split(" "); if (arr.size == 1) arr(1) else "[error]"})
    val urls = requests.map(x => {
      val arr = x.toString.split(" ")
      if (arr != "-" && arr.size > 1)
        arr(0)
      else
        "[error]"
    })
    // Reduce by URLm over a 5 minute widnow slding every second
    val urlCounts = urls.map( x=> (x,1)).reduceByKeyAndWindow(_+_, _-_, Seconds(300), Seconds(1))

    // Sort and print the results
    val sortedResults = urlCounts.transform(rdd => rdd.sortBy(x => x._2, false))

    sortedResults.print()

    ssc.checkpoint("/Users/Jake/Desktop/Jake_Programming/Scala/StreamingProcessingProjs/src/CPLogParse")

    ssc.start()
    ssc.awaitTermination()



  }
}
