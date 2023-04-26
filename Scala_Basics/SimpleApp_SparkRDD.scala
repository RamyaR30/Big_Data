package org.softlang.bigdata.ass3

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object SimpleApp {

  def main(args: Array[String]): Unit = {
    // Setup spark.
    val spark = SparkSession
      .builder()
      .master("local[8]") // Local Spark instance with 8 cores.
      .appName("Simple Application")
      .getOrCreate()

    // We start with plain Spark (sql will follow later).
    val sc = spark.sparkContext

    // To get rid of extensive tracking messages (optional)
    sc.setLogLevel("ERROR")

    // Our local data to start with.
    val input: Seq[Int] = 1 to 100

    // Get input data to the cloud.
    val myFirstRDD: RDD[Int] = sc.parallelize(input)

    // Do some processing.
    val mySecondRDD: RDD[Int] = myFirstRDD.map(x => x + 1)

    // Fetch output data from the cloud.
    val output: Array[Int] = mySecondRDD.collect()

    // Print the data, local again, to check results.
    println(output.mkString(","))

    // Stop spark.
    spark.stop()
  }
}
