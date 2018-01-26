package LogParse

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext, Time}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.sql.SQLContext
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext

import java.util.regex.Pattern
import java.util.regex.Matcher

import TwitUtil.Utilities._

case class Record(url:String, status:Int, agent:String)

object SQLContextSingleton{
  @transient private var instance: SQLContext = _

  def getInstance(sparkContext : SparkContext):SQLContext = {
    if(instance == null)
      instance = new SQLContext(sparkContext)
    instance
  }
}

object LogSQL {


  def main(args:Array[String]): Unit = {
    // Create the context with a 1 secon batch size
    val conf = new SparkConf ().setAppName("LogSQL").setMaster("local[*]").set("spark.sql.warehouse.dir",
      "/Users/Jake/Desktop/Jake_Programming/Scala/StreamingProcessingProjs/src/CPSqlLog")

    val ssc = new StreamingContext(conf, Seconds(1))

    setupLogging()

    // Construct a regular expression (regex) to extract fields from raw Apache log lines
    val pattern = apacheLogPattern()

    // Create a socket stream to read log data published via netcat on port 9999 locally
    val lines = ssc.socketTextStream("127.0.0.1", 9999, StorageLevel.MEMORY_AND_DISK_SER)

    // Extract the (URL, status, user agent) we want from each log line
    val requests = lines.map(x => {
      val matcher : Matcher = pattern.matcher(x)

      if(matcher.matches())
      {
        val request = matcher.group(5)
        val reqeustField = request.toString().split(" ")
        val url = util.Try(reqeustField(1)) getOrElse( "[error]")

        (url, matcher.group(6).toInt, matcher.group(9))

      }else {
        ("error", 0, "error")
      }
    })

    // Process each RDD from each batch as it comes in

    requests.foreachRDD((rdd, time) =>{

      // Get the singleton instance of SQLContext
      val sQLContext = SQLContextSingleton.getInstance(rdd.sparkContext)
      import sQLContext.implicits._

      // SparkSQL can automatically create DataFrames from Scala "case classes".
      // We created the Record case class for this purpose.
      // So we'll convert each RDD of tuple data into an RDD of "Record"
      // objects, which in turn we can convert to a DataFrame using toDF()
      val requestDataFrame = rdd.map(w=> Record(w._1, w._2, w._3)).toDF()

      requestDataFrame.registerTempTable("requests")

      val wordCountsDataFrame =
        sQLContext.sql("select agent, count(*) as total from requests group by agent")
      wordCountsDataFrame.show()


    })


    ssc.start()
    ssc.awaitTermination()
  }
}

