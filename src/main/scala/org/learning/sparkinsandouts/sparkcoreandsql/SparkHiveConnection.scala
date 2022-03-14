package org.learning.sparkinsandouts.sparkcoreandsql

import org.apache.spark.sql.SparkSession

object SparkHiveConnection {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("Spark Hive Connection")
      .master("local[*]")
      .config("hive.metastore.uris", "thrift://localhost:9083")
      .enableHiveSupport()
      .getOrCreate()

    import spark.implicits._

    val empDF = spark.sql("select * from default.student_txn where gender='Male'")
    empDF.show()
  }
}
