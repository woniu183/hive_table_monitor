  package Util

  import java.nio.charset.Charset
  import java.sql.{Connection, DriverManager}

  import scala.collection.mutable.ListBuffer


  object HiveJdbcUtil {

    var connection: Connection = null

    private val driverName = "org.apache.hive.jdbc.HiveDriver"
    Class.forName(driverName)

    def getConnection(host: String, port: Int, user: String, password: String): Connection = {
      val url = "jdbc:hive2://" + host + ":" + port.toString + "/" +"?useUnicode=true&characterEncoding=UTF-8"
      connection = DriverManager.getConnection(url, user, password)
      connection
    }

    def Connection4db(host: String, port: Int, user: String, password: String, db: String): Connection = {
      val url = "jdbc:hive2://" + host + ":" + port.toString + "/" + db+"?useUnicode=true&characterEncoding=UTF-8"
      DriverManager.getConnection(url, user, password)
    }

    def close(host: String, port: Int, user: String, password: String): Unit = {
      connection.close()
    }

    def getDataBases(con: Connection): List[String] = {
      val listBuffer = new ListBuffer[String]()
      val stmt = con.createStatement
      val res = stmt.executeQuery("show databases")
      while ( {
        res.next
      }) {
        val value = res.getString(1)
        println(value)
        listBuffer.+=:(value)
      }
      listBuffer.toList
    }

    def getTables4DB(connection2db: Connection): List[String] = {
      val listBuffer = new ListBuffer[String]()
      val stmt = connection2db.createStatement
      val res = stmt.executeQuery("show tables")
      while ( {
        res.next
      }) {
        val value = res.getString(1)
        println(value)
        listBuffer.+=:(value)
      }
      stmt.close()
      listBuffer.toList
    }

    def getShowCreateTable(db: String, table: String): String = {
      var result: String = null
      val stmt = connection.createStatement
      val res = stmt.executeQuery("show create table " + db + "." + table)
      while ( {
        res.next
      }) {
        val value = res.getString(1)
        println(value)
        result = value
      }
      stmt.close()
      result

    }

    def getTableLocation(conn: Connection, db: String, table: String): String = {
      val result:StringBuffer =new StringBuffer()
      val stmt = conn.createStatement
      val res = stmt.executeQuery("show create table " + db + "." + table)
     
      while ( {
        res.next
      }) {
        val value = res.getString(1)
        result.append(value)
      }
      stmt.close()
     result.toString.split("LOCATION").apply(1).split('\'').apply(1)
    }

       /**
      def main(args: Array[String]): Unit = {
       // val  con = DriverManager.getConnection("jdbc:hive2://172.172.240.1:10000/default", "hive", "")

       val  conn1= getConnection("172.172.240.1", 10000, "hive", "")
        /**
        val stmt = con.createStatement
        val  res = stmt.executeQuery("show databases")
        while ( {
          res.next
        }) println(res.getString(1))*/

       // getDataBases(connection)
       //val conn4fdm= Connection4db("172.172.240.1", 10000, "hive", "","fdm")
       //  getTables4DB(conn4fdm)

       // getShowCreateTable("fdm","fdm_wxbot_group_list_da")
       println( getTableLocation(conn1,"fdm","fdm_wxbot_group_list_da")  )
        conn1.close()

      }
         **/

  }


