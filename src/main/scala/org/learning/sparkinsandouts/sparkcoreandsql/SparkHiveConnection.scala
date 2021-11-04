package org.learning.sparkinsandouts.sparkcoreandsql

import org.apache.spark.sql.SparkSession

object SparkHiveConnection {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("Spark Hive Connection")
      .master("local[*]")
      .config("hive.metastore.uris", "thrift://localhost:9083")
      //    .config("javax.jdo.option.ConnectionUserName", "hive")
      //    .config("javax.jdo.option.ConnectionPassword", "H1AQJFaRQ67J8nf9")
      .enableHiveSupport() // don't forget to enable hive support
      .getOrCreate()

    import spark.implicits._

    val empDF = spark.table("employee")
    empDF.show()
  }
}
