package ClassMaterial.Seqfile_ex


import org.apache.hadoop.io.{IntWritable, NullWritable, Text}
import org.apache.spark.{SparkConf, SparkContext}
object Sequence_Ex {

  def main(args:Array[String]): Unit ={

    val sc = new SparkContext(new SparkConf().setAppName("SEQ_EX").setMaster("local"))

    //val productsRdd = sc.textFile("./retail_db/products")

    //productsRdd.map(x => (NullWritable.get(), x)).saveAsSequenceFile("./Sequences")

    //sc.sequenceFile("./Sequences", classOf[NullWritable], classOf[Text]).map(x=> x._2.toString()).foreach(println)



    val products = sc.textFile("./retail_db/products")
    val productsMap = products.map(x => (new IntWritable(x.split(",")(0).toInt), new Text(x)))

    import org.apache.hadoop.mapreduce.lib.output._

    productsMap.saveAsNewAPIHadoopFile("./NewSeq", classOf[IntWritable], classOf[Text], classOf[SequenceFileOutputFormat[IntWritable, Text]])

    import org.apache.hadoop.mapreduce.lib.input._
    //sc.newAPIHadoopFile("./NewSeq", classOf[IntWritable], classOf[Text], classOf[SequenceFileOutputFormat[IntWritable, Text]])



  }

}
