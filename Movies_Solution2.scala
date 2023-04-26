package org.bigdata.tutorial

import org.apache.spark.sql.{DataFrame, SparkSession}

object Assignment52Tutorial1 {

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

    val languageSpecificCountsSorted = df.groupBy("original_language").count()
      .sort(col("count").desc)

    val genreSpecificCountsSorted = df.groupBy("genres").count()
      .sort(col("count").desc)

    //val top10Countery = languageSpecificCountsSorted.take(10).map(x => x.getString(1))

    val top10Country: DataFrame = languageSpecificCountsSorted.limit(10).drop("count")

    val dfLimitedToTop10Country = df.join(top10Country, usingColumns = Seq("original_language"))

    val genreLanguage = dfLimitedToTop10Country
      .groupBy("genres")
      .pivot("original_language").count()
      .sort(col("en").desc)

    //    dfLimitedToTop10Country.groupBy("original_language").count()
    //      .sort(col("count").desc).show()

    dfLimitedToTop10Country
      .groupBy("original_language", "genres").count()
      .join(languageSpecificCountsSorted.withColumnRenamed("count", "total"), usingColumns = Seq("original_language"))
      .withColumn("fraction", round(col("count") / col("total"), 2))
      .groupBy("genres").pivot("original_language").sum("fraction")
      .sort(col("en").desc)
      .show()



    //genreLanguage.show()

  }


}
