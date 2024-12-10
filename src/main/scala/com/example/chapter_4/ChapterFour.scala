package com.example.chapter_4

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._

object ChapterFour {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local[*]").appName("ChapterFourApp").getOrCreate()

    val path = args(0)

    // use NYC taxi as dataset for this chapter
    val schema =
      StructType(
        Array(
          StructField("VendorID", IntegerType, nullable = true),
          StructField("tpep_pickup_datetime", TimestampType, nullable = true),
          StructField("tpep_dropoff_datetime", TimestampType, nullable = true),
          StructField("passenger_count", IntegerType, nullable = true),
          StructField("trip_distance", DoubleType, nullable = true),
          StructField("pickup_longitude", DoubleType, nullable = true),
          StructField("pickup_latitude", DoubleType, nullable = true),
          StructField("RateCodeID", IntegerType, nullable = true),
          StructField("store_and_fwd_flag", StringType, nullable = true),
          StructField("dropoff_longitude", DoubleType, nullable = true),
          StructField("dropoff_latitude", DoubleType, nullable = true),
          StructField("payment_type", IntegerType, nullable = true),
          StructField("fare_amount", DoubleType, nullable = true),
          StructField("extra", DoubleType, nullable = true),
          StructField("mta_tax", DoubleType, nullable = true),
          StructField("tip_amount", DoubleType, nullable = true),
          StructField("tolls_amount", DoubleType, nullable = true),
          StructField("improvement_surcharge", DoubleType, nullable = true),
          StructField("total_amount", DoubleType, nullable = true)
        )
      )

    val df = spark.read.schema(schema).option("header", "true").csv(path)

    df.show(10, truncate = false)

    df.createOrReplaceTempView("taxi_data")
    spark
      .sql(
        "SELECT VendorID, MAX(total_amount) as max_total_amount " +
          "FROM taxi_data WHERE trip_distance <= 4 GROUP BY VendorID"
      )
      .show(100, truncate = false)
  }
}
