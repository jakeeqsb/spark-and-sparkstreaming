package Ex2

import java.sql._;
import scala.Array;


object JDBC_Connection {


  def main(args:Array[String]): Unit ={

    var connection:Connection = null;
    var stmt:Statement = null;
    var rs:ResultSet = null

    val host = "localhost"
    val port = "3306"
    val db = "tysql"
    val driver = "com.mysql.jdbc.Driver"
    val url = "jdbc:mysql://" + host + port + "/" + db
    val user = "jake"
    val password = "-----"

    try{

      Class.forName(null)
      connection = DriverManager.getConnection(url, user, password)

      stmt = connection.createStatement()
      rs = stmt.executeQuery("select * from Customer")

      while(rs.next())
        {
          val name = rs.getString("cust_name")
          val addr = rs.getString("cust_address")

          println("name, addr = " + name + "," + addr)
        }

    }
    catch{
      case e => e.printStackTrace()
    }
    finally{
      connection.close()
    }

  }
}
