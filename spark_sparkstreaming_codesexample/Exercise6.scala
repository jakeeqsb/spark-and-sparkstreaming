package Ex6

import org.apache.spark.{SparkConf, SparkContext}

object Exercise6 {


  def topNStock(rec: (String, Iterable[String]), topN : Int) : Iterable[String] = {
    val stockList = rec._2.toList

    val topNPrices = stockList.
      map (x => x.split(",")(2).toLong).
      sortBy(x => -x ).distinct.slice(0, topN)

    val topNPriceStock = stockList.sortBy(x => x.split(",")(2).toLong).
      filter(x=> topNPrices.contains(x.split(",")(2).toFloat))

    topNPriceStock

  }


  def main(args:Array[String]){


    val sc = new SparkContext(new SparkConf().setAppName("topNStocks").setMaster("local"))

    val stocksRdd = sc.textFile("./nyse_data/nyse_2009.csv")

    //stocksRdd.take(10).foreach(println)

    val stocksMap = stocksRdd.map(x => ((x.split(",")(0),(x.split(",")(1)).split("-")(1)), x.split(",")(6).toLong))

    val stockGBK = stocksMap.groupByKey()

    //stockGBK.take(10).foreach(println)

  }
}
