package com.example

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

object CountCandies {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .master("local[*]")
      .appName("MacFirstSparkApp")
      .getOrCreate()

    if (args.length < 1) {
      println("Usage: CountCandies <mmm_file_dataset>")
      sys.exit(1)
    }
    val sourceName = args(0)
    val df = spark
      .read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(sourceName)

    val countMnmDf = df
      .select("State", "Color", "Count")
      .groupBy("State", "Color")
      .agg(count("Count").alias("Total"))
      .orderBy(desc("Total"))

    countMnmDf.show(10, false)
    spark.stop()
  }
}
