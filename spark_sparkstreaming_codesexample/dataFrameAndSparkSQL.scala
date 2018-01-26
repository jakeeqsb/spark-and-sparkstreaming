package ClassMaterial.Seqfile_ex

import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.sql._
import org.apache.spark.sql.functions._

object dataFrameAndSparkSQL {

  case class Orders( order_id: Int,
                     order_date: String,
                     order_customer_id: Int,
                     order_status: String)


  case class OrderItems(order_item_id: Int, order_item_order_id: Int, order_item_product_id: Int,
                        order_item_quantity: Int, order_item_subtotal: Float, order_item_price: Float)


  def main(args: Array[String]){
    val conf = new SparkConf().
      setAppName("Total Revenue - Daily - Data Frames").setMaster("local")

    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    sqlContext.setConf("spark.sql.shuffle.partitions", "2")

    import sqlContext.implicits._

    val ordersDF = sc.textFile("./retail_db" + "/orders").
      map(rec => {
        val a = rec.split(",")
        Orders(a(0).toInt, a(1).toString(), a(2).toInt, a(3).toString())
      }).toDF()


    val orderItemsDF = sc.textFile("./retail_db" + "/order_items").
      map(rec => {
        val a = rec.split(",")
        OrderItems(
          a(0).toInt,
          a(1).toInt,
          a(2).toInt,
          a(3).toInt,
          a(4).toFloat,
          a(5).toFloat)
      }).toDF()


    val ordersFiltered = ordersDF.
      filter(ordersDF("order_status") === "COMPLETE")

    val ordersJoin = ordersFiltered.join(orderItemsDF,
      ordersFiltered("order_id") === orderItemsDF("order_item_order_id"))


    ordersJoin.
      groupBy("order_date").
      agg(sum("order_item_subtotal")).
      sort("order_date").
      rdd.
      saveAsTextFile("./dataFramResult")

  }
}
