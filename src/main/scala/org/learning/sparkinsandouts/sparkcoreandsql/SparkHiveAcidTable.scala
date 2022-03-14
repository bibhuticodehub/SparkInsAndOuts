package org.learning.sparkinsandouts.sparkcoreandsql

import org.apache.spark.sql.{SparkSession, DataFrame}

object SparkHiveAcidTable {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .appName("Spark Hive ACID Table Connection")
      .master("local[*]")
      .getOrCreate()

    val query = "select * from student_txn"

    val df = spark.read
      .format("jdbc")
      .option("url","jdbc:hive2://localhost:10000/default")
      .option("dbtable",s"($query) tmp")
      .option("user","root")
      .option("password","root@123")
      .load()

    df.show()
  }
}
