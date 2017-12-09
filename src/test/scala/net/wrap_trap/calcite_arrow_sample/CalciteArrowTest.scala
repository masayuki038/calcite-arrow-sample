package net.wrap_trap.calcite_arrow_sample

import java.sql.DriverManager

import net.wrap_trap.calcite_arrow_sample.Helper._
/**
  * Created by masayuki on 2017/12/09.
  */
object CalciteArrowTest {

  def main(args: Array[String]): Unit = {
    Class.forName("org.apache.calcite.jdbc.Driver")
    val conn = DriverManager.getConnection("jdbc:calcite:model=target/scala-2.12/classes/model.json", "admin", "admin")
    using(conn.prepareStatement("select N_NATIONKEY, N_NAME from NATIONSSF")) { pstmt =>
      using(pstmt.executeQuery()) { rs =>
        while (rs.next) {
          println("N_NATIONKEY: %d, N_NAME: %s".format(rs.getLong("N_NATIONKEY"), rs.getString("N_NAME")))
        }
      }
    }
  }
}
