package org.learning.sparkinsandouts.sparkstreaming

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.Trigger

object SparkStreamingFileSource {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .master("local")
      .appName("Streaming Word Count")
      .config("spark.streaming.stopGracefullyOnShutdown", "true")
      .config("spark.sql.streaming.schemaInference", "true")
      .config("spark.sql.shuffle.partitions", 5)
      .getOrCreate()

    val rawDF = spark.readStream
      .format("json")
      .option("path", "input")
      .load()

    val explodedDF = rawDF.selectExpr("InvoiceNumber",
      "StoreID",
      "PosID",
      "DeliveryAddress.AddressLine",
      "DeliveryAddress.City",
      "DeliveryAddress.State",
      "DeliveryAddress.PinCode",
      "explode(InvoiceLineItems) as LineItem")

    val finalDF = explodedDF
      .withColumn("ItemCode", expr("LineItem.ItemCode"))
      .withColumn("ItemDescription", expr("LineItem.ItemDescription"))
      .withColumn("ItemPrice", expr("LineItem.ItemPrice"))
      .withColumn("ItemQty", expr("LineItem.ItemQty"))
      .withColumn("TotalValue", expr("LineItem.TotalValue"))

    val transformedDFQuery = finalDF
      .writeStream
      .format("json")
      .option("path", "output")
      .option("checkpointLocation", "chk-point-dir")
      .outputMode("append")
      .queryName("File Input Stream")
      .trigger(Trigger.ProcessingTime("1 minute"))
      .start()

    transformedDFQuery.awaitTermination()
  }
}
