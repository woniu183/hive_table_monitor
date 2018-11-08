package Util
import java.sql.Connection
import java.sql.DriverManager


object igniteTest {
  def main(args: Array[String]): Unit = {
    Class.forName("org.apache.ignite.IgniteJdbcThinDriver")
    val conn = DriverManager.getConnection("jdbc:ignite:thin://172.172.178.210:10800/")
    // Get data// Get data

    try {
      val stmt = conn.createStatement
      try
        try {
          val rs = stmt.executeQuery("select ID,NAME from CITY")
          try
              while ( {
                rs.next
              }) System.out.println(rs.getString(1) + ", " + rs.getString(2))
          finally if (rs != null) rs.close()
        }
        finally if (stmt != null) stmt.close()
    }
  }

}
