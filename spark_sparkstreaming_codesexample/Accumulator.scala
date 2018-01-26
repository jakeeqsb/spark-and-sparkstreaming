package ClassMaterial.Seqfile_ex


import org.apache.spark.{SparkConf, SparkContext}

object Accumulator {


  def main(args:Array[String]):Unit={
    val sc = new SparkContext(new SparkConf().setAppName("Shared_Variable").setMaster("local"))
    val file = sc.textFile ("./WordCount/wordCount.txt")
    var numBlankLines = sc.accumulator(0)



    def toWords(line: String): Array[String] = {

      if(line.length() == 0) {numBlankLines += 1}

      return line.split(" ");
    }

    var words= file.flatMap(toWords)
    words.saveAsTextFile("./WordCountResult")
    println(numBlankLines)


  }
}
