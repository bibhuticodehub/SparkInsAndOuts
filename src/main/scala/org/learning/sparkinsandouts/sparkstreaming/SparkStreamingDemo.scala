package org.learning.sparkinsandouts.sparkstreaming

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object SparkStreamingDemo {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .master("local")
      .appName("Streaming Word Count")
      .config("spark.streaming.stopGracefullyOnShutdown", "true")
      .config("spark.sql.shuffle.partitions", 3)
      .getOrCreate()

    val lineDF = spark.readStream
      .format("socket")
      .option("host", "localhost")
      .option("port", "9999")
      .load()

    val countsDF = lineDF.withColumn("words", explode(split(col("value"), " ")))
      .drop(col("value"))
      .groupBy("words").count()

    val wordCountQuery = countsDF.writeStream
      .format("console")
      .option("checkpointLocation", "chk-point-dir")
      .outputMode("complete")
      .start()

    wordCountQuery.awaitTermination()
  }
}
