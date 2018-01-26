import org.apache.spark.{SparkContext, SparkConf}

object Exercise5_1 {


  def main(args: Array[String])={

    val inputPath:String = "./src/retail_db"

    val sc = new SparkContext (new SparkConf().setAppName("Exercise 5-1 ").setMaster("local"))

    //RDD[(order_id: Int, order_date: String)]
    val orders = sc.textFile(inputPath+"/orders")
      .filter(rec => (rec.split(",")(3) == "COMPLETE" || rec.split(",")(3) == "CLOSED"))
      .map(rec => (rec.split(",")(0).toInt, rec.split(",")(1)))

    //RDD[(order_item_order_id: Int, (order_item_product_id: Int, order_item_subtotal: Float))]
    val orderItems = sc.textFile(inputPath+"/order_items")
      .map(rec => (rec.split(",")(1).toInt,(rec.split(",")(2).toInt,rec.split(",")(4).toFloat)))

    //RDD[(product_id: Int, product_category_id: Int)]
    val products = sc.textFile(inputPath+"/products")
      .map(rec => (rec.split(",")(0).toInt,rec.split(",")(1).toInt))

    //RDD[(category_id: Int, category_department_id: Int)]
    val categories = sc.textFile(inputPath+"/categories")
      .map(rec => (rec.split(",")(0).toInt,rec.split(",")(1).toInt))

    //RDD[(department_id: Int, department_name: String)]
    val departments = sc.textFile(inputPath+"/departments")
      .map(rec => (rec.split(",")(0).toInt,rec.split(",")(1)))

    //RDD[(order_item_order_id: Int, ((order_item_product_id: Int, order_item_subtotal: Float), order_date: String))]
    val ordersJoin = orderItems.join(orders)
    //RDD[(order_item_product_id: Int, (order_date: String, order_item_subtotal: Float))]
    val ordersJoinMap = ordersJoin.map(rec => (rec._2._1._1,(rec._2._2,rec._2._1._2)))

    //Rolling up from ORDER_ITEMS to CATEGORIES through PRODUCTS
    //RDD[(product_category_id: Int, (order_date: String, order_item_subtotal: Float))]
    val ordersCategories = ordersJoinMap.join(products).map(rec => (rec._2._2,(rec._2._1)))

    //Rolling up from ORDER_ITEMS to DEPARTMENTS through CATEGORIES
    //RDD[(category_department_id: Int, (order_date: String, order_item_subtotal: Float))]
    val ordersDepartments = ordersCategories.join(categories).map(rec => (rec._2._2,rec._2._1))

    //RDD[(order_date+"\t"+department_name: String, order_item_subtotal: Float)]
    val ordersFinal = ordersDepartments.join(departments).map(rec => (rec._2._1._1+"\t"+rec._2._2,rec._2._1._2.toFloat))

    //Calculate the dailyRevenue with key as date+department_name
    //RDD[(String, Float)]
    val dailyRevenuePerDepartment = ordersFinal.reduceByKey((a,b) => a+b)

    //Publish the data for reporting purposes with date formatted
    //RDD[(String, String, Float)]
    val reportingData = dailyRevenuePerDepartment
      .map(rec => (rec._1.split("\t")(0).substring(0,10),rec._1.split("\t")(1),rec._2))


    reportingData.take(10).foreach(println)

  }
}
