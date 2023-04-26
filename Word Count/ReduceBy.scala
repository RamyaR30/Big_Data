package org.bigdata.tutorial

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, ObjectInputStream, ObjectOutputStream}
import scala.util.Random

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object Tutorial2 {
  /**
   * The following methods simulates fake "big data".
   * It draws a random word out of the list ("word_0", "word_1" ... "word_n").
   * The idx is taken as seed, to produce the same word for the same index when repeating the call.
   */
  def generator(idx: Long, n: Int): String = {
    // Initialize random with seed.
    val random = new Random(idx)

    // Produce a word with up to n different options.
    "word_" + random.nextInt(n)
  }

  /**
   * This is a small helper that takes two immutable maps and sums up the values for the corresponding keys.
   * You need it for the map-reduce solution.
   */
  def op(l: Map[String, Int], r: Map[String, Int]): Map[String, Int] = {
    (l.keySet ++ r.keySet).map(key =>
      (key, l.getOrElse(key, 0) + r.getOrElse(key, 0))
    ).toMap
  }

  def serialise(value: Any): Array[Byte] = {
    val stream: ByteArrayOutputStream = new ByteArrayOutputStream()
    val oos = new ObjectOutputStream(stream)
    oos.writeObject(value)
    oos.close()
    stream.toByteArray
  }

  def deserialise[T](bytes: Array[Byte]): T = {
    val ois = new ObjectInputStream(new ByteArrayInputStream(bytes))
    val value = ois.readObject
    ois.close()
    value.asInstanceOf[T]
  }

  // Scala calls are different, compared to Spark, so I patched it.
  // You dont need to understand this code.
  // Only be sure that this code is in your file, if you want to use reduceByKey syntax, on Scala's Seq.
  implicit class BetterSeq[K, V](val xs: Seq[(K, V)]) {
    def reduceByKey(op: (V, V) => V): Map[K, V] = xs.groupMapReduce(x => x._1)(x => x._2)(op)
  }

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .master("local[2]") // Local Spark instance with 8 cores.
      .appName("Simple Application")
      .getOrCreate()

    val sc = spark.sparkContext

    // Word count again.
    val size = 100
    val n = 10

    val ourStream: Seq[String] = (0 to size).map(x => generator(x, n))

    // Different local Scala solutions:
    //    val sol1 = ourStream.map(word => Map((word, 1))).reduce((l, r) => op(l, r))
    //    val sol2 = ourStream.map(word => Map((word, 1))).fold(Map())((l, r) => op(l, r))
    //    val sol3 = ourStream.groupBy(word => word).map { case (key, seq) => (key, seq.length) }
    //    val sol4 = ourStream.map { word => (word, 1)}.reduceByKey { (l, r) => l + r  }

    // RDD version of solution 4 (with slowdown).
    val ourFirstRdd: RDD[String] = sc.parallelize(ourStream, 20)

    val ourSecondRdd: RDD[(String, Int)] = ourFirstRdd
      .map { word =>
        Thread.sleep(1000)
        (word, 1)
      }.reduceByKey { (l, r) =>
      Thread.sleep(1000)
      l + r
    }

    val ourLocalCounter: Array[(String, Int)] = ourSecondRdd.collect()

    println(ourLocalCounter.mkString("Array(", ", ", ")"))

    // To prevent JVM from shutting down.
    System.in.read()
  }

}
