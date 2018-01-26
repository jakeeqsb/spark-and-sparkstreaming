package Integration


import java.io.{BufferedReader, InputStreamReader}
import java.net.Socket
import java.nio.charset.StandardCharsets

import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.receiver.Receiver

import java.util.regex.Pattern
import java.util.regex.Matcher

import TwitUtil.Utilities._


class CustomReceiver(host:String, port:Int)
  extends Receiver[String](StorageLevel.MEMORY_AND_DISK_2) {

  def onStart(): Unit ={

    new Thread("Socket Receiver"){
      override def run() {receive()}
    }.start()

  }
  def onStop() {}

  private def receive(): Unit ={

    var socket:Socket = null
    var userInput:String = null

    try{

      socket = new Socket(host, port)

      val reader = new BufferedReader(new InputStreamReader(socket.getInputStream(), "UTF-8"))
      userInput = reader.readLine()

      while(!isStopped() && userInput != null )
      {
        store(userInput)
        userInput = reader.readLine()
      }
      reader.close()
      socket.close()
    }catch {
      case e: java.net.ConnectException =>
        restart("Error connecting to " + host +":" + port, e)
      case t: Throwable =>

        restart("Error receiving data", t)
    }

  }

}

object CustomReceiverExample {

  def main(args: Array[String]): Unit ={
    val ssc = new StreamingContext("local[*]", "CustomerReceiverExample", Seconds(1))

    setupLogging()

    val pattern = apacheLogPattern()

    val lines = ssc.receiverStream(new CustomReceiver("localhost", 7777))

    lines.print()

    /*
    // Extract the request field from each log line
    val requests = lines.map(x => {val matcher:Matcher = pattern.matcher(x); if (matcher.matches()) matcher.group(5)})

    // Extract the URL from the request
    val urls = requests.map(x => {val arr = x.toString().split(" "); if (arr.size == 3) arr(1) else "[error]"})

    // Reduce by URL over a 5-minute window sliding every second
    val urlCounts = urls.map(x => (x, 1)).reduceByKeyAndWindow(_ + _, _ - _, Seconds(300), Seconds(1))

    // Sort and print the results
    val sortedResults = urlCounts.transform(rdd => rdd.sortBy(x => x._2, false))
    sortedResults.print()

    ssc.checkpoint("/Users/Jake/Desktop/Jake_Programming/Scala/StreamingProcessingProjs/src/CPCustomReceiver")
    */
    ssc.start()
    ssc.awaitTermination()

  }

}
