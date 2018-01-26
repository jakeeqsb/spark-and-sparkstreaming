package ClassMaterial.Seqfile_ex

/* spark submit command
spark-submit --class StreamingDepartmentAnalysis \
  --master yarn \
  --conf spark.ui.port=22231 \
  --jars "/usr/hdp/2.5.0.0-1245/spark/lib/spark-streaming_2.10-1.6.2.jar,
  /usr/hdp/2.5.0.0-1245/kafka/libs/spark-streaming-kafka_2.10-1.6.2.jar,
  /usr/hdp/2.5.0.0-1245/kafka/libs/kafka_2.10-0.8.2.1.jar,
  /usr/hdp/2.5.0.0-1245/kafka/libs/metrics-core-2.2.0.jar" \
  retail_2.10-1.0.jar /user/dgadiraju/streaming/streamingdepartmentanalysis
*/
/*
 flume and kafka integration configuration file
 kandf.sources = logsource
 kandf.sinks = ksink
 kandf.channels = mchannel

 # describe / configure the source
 kandf.sources.logsource.type = exec
 kandf.sources.logsource.command = tail -F /opt/gen_logs/logs/access.log

 # Describe the sink
 kandf.sinks.ksink.type = org.apache.flume.kafka.KafkaSink
 kandf.sinks.ksink.brokerList = nn02.itversity.com:6667
 kandf.sinks.ksink.topic = kafkadg

 # Use a channel which buffers events in memory
 kandf.channels.mchannel.type = memory
 kandf.channels.mchannel.capacity = 1000
 kandf.channels.mchannel.transactionCapacity = 100

 # Bind the source and sink to the channel
 kandf.sources.logsource.channel = mchannel
 kandf.sinks.ksink.channel = mchannel

*/

import kafka.serializer.StringDecoder
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka._
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.InputDStream

object FlumeKfakaCluster {

  def main(args: Array[String]) {
    val sparkConf = new SparkConf().setAppName("DepartmentWiseCount").setMaster("yarn-client")
    val topicsSet = "kafkadg".split(",").toSet
    val kafkaParams =
      Map[String, String]("metadata.broker.list" -> "nn01.itversity.com:6667,nn02.itversity.com:6667,rm01.itversity.com:6667")

    val ssc = new StreamingContext(sparkConf, Seconds(30))
    val messages: InputDStream[(String, String)] = KafkaUtils.
      createDirectStream[String, String, StringDecoder, StringDecoder](
      ssc, kafkaParams, topicsSet)


    val lines = messages.map(_._2)
    val linesFiltered = lines.filter(rec => rec.contains("GET /department/"))
    val countByDepartment = linesFiltered.
      map(x => (x.split(" ")(6).split("/")(2),1)).
      reduceByKey(_+_)
    //        reduceByKeyAndWindow((a:Int, b:Int) => (a + b), Seconds(300), Seconds(60))
    //    countByDepartment.saveAsTextFiles(args(0))
    // Below function call will save the data into HDFS
    //countByDepartment.saveAsTextFiles(args(0))
    ssc.start()
    ssc.awaitTermination()

  }


}
