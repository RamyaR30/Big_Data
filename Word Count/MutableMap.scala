package org.bigdata.tutorial

import java.io.FileWriter
import scala.collection.{Seq, mutable}
import scala.util.Random

object Assignment1_1 {

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

  def main(args: Array[String]): Unit = {
    val size = 100
    val n = 10
    val ourStream: Seq[String] = (0 to size).map(x => generator(x, n))
    val ourMutableMap = scala.collection.mutable.Map[String, Int]().withDefaultValue(0)
    ourStream.foreach(word =>ourMutableMap(word) += 1)
   
    val fw = new FileWriter("output.csv")
    for ((word, count) <- ourMutableMap){
      fw.write(word + "," + count + "\n")
    }
    fw.flush()
    fw.close()
  }
}