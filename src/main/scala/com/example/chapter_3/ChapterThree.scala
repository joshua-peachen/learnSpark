package com.example.chapter_3

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._

object ChapterThree {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .master("local[*]")
      .appName("ChapterThreeSparkApp")
      .getOrCreate()

    val sc = spark.sqlContext
    import sc.implicits._

    val resourcePath = args(0)
    val schema = StructType(
      Array(
        StructField("id", IntegerType, nullable = false),
        StructField("data_str", StringType, nullable = false),
        StructField("data_double", DoubleType, nullable = false)
      )
    )

    val ds = spark.read
      .option("header", "true")
      .schema(schema)
      .csv(resourcePath)
      .as[SourceCsv]

    ds.show(10, truncate = false)

    ds.filter(_.id % 2 == 0).show(10, truncate = false)

    ds.explain()

    spark.stop()
  }
}

case class SourceCsv(id: Int, data_str: String, data_double: Double)
