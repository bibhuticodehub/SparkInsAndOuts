package org.learning.sparkinsandouts

import org.apache.spark.sql.SparkSession

object ExploringAzure {
  def main(args: Array[String]): Unit = {
    val Array(dbName,
    inputPath,
    outputTableName,
    outputPath) = args

    val spark = SparkSession.builder().enableHiveSupport().getOrCreate()

    val df = spark.read.option("header", true).csv(inputPath)

    df.write.format("parquet").mode("overwrite")
      .option("path", outputPath)
      .saveAsTable(dbName + "." + outputTableName)
  }
}
