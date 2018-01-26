package ClassMaterial.Seqfile_ex

import org.apache.spark.{SparkConf,SparkContext}

object BroadcastVar {

  def main(args:Array[String]){
    // we need remove the common words from our wordcount, what do we need to do
    val sc = new SparkContext(new SparkConf().setAppName("Broadcast EX").setMaster("local"))
    var commonWords = Array("a", "an","the","of","at","is","am","are","this","that","at","in","or","and",
                            "not","be","for","to","it")

    val commonWordsMap = collection.mutable.Map[String, Int]()

    for(word <- commonWords){
      commonWordsMap(word) = 1
    }

    var commonWordsBC = sc.broadcast(commonWordsMap)

    var file = sc.textFile("./WordCount")

    def toWord(line:String):Array[String] = {
      var words = line.split(" ")
      var output = Array[String]()

      for(word <- words) {
        if( ! (commonWordsBC.value.contains(word.toLowerCase.trim.replaceAll("[^a-z]",""))))
          output = output :+ word

      }
      return output
    }
  
    var uncommonWords = file.flatMap(toWord)

    uncommonWords.take(10).foreach(println)
  }


}
