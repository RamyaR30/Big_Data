package org.bigdata.tutorial

import org.apache.spark.sql.{DataFrame, SparkSession}

object Assignment52Tutorial2 {

  def main(args: Array[String]): Unit = {

    // Setup spark.
    val spark = SparkSession
      .builder()
      .master("local[8]") // Local Spark instance with 8 cores.
      .appName("Simple Application")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    import org.apache.spark.sql.functions._

    // Needed to access the relevant spark SQL methods.

    val df: DataFrame = spark.read.format("csv")
      .option("header", "true")
      .load("data/movies.csv")

    val languageSpecificMovieCount = df.groupBy("original_language").count()
      .sort(col("count").desc)

    val genreSpecificMovieCount = df.groupBy("genres").count()
      .sort(col("count").desc)

    val top10 = languageSpecificMovieCount.limit(10).drop("count")

    val dfTop10 = df.join(top10, usingColumns = Seq("original_language"))

    dfTop10.groupBy("genres").pivot("original_language").count()
      .sort(col("en").desc)
    //.show()

    val temp = dfTop10.groupBy("genres", "original_language").count()
      .join(languageSpecificMovieCount.withColumnRenamed("count", "total"),
        usingColumns = Seq("original_language"))
      .withColumn("fraction", round(col("count") / col("total"), 2))

    temp.groupBy("genres").pivot("original_language").sum("fraction")
      .sort(col("en").desc)
      .show()

  }


}
