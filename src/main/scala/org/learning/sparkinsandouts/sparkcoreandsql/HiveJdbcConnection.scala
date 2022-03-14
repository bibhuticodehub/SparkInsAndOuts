package org.learning.sparkinsandouts.sparkcoreandsql

import java.sql.DriverManager
import org.apache.hive.jdbc.HiveDriver

object HiveJdbcConnection {

  def main(args: Array[String]): Unit = {
    val driverName = "org.apache.hive.jdbc.HiveDriver"
    Class.forName(driverName)
    val con = DriverManager.getConnection("jdbc:hive2://localhost:10000/default", "root", "root@123")
    val stmt = con.createStatement()
    val tableName = "student_txn"
    val sql = "select * from " + tableName;
    val res = stmt.executeQuery(sql);
    while (res.next()) {
      System.out.println(String.valueOf(res.getInt(1)) + "\t" + res.getInt(2)+"\t" + res.getString(3));
    }
  }
}

