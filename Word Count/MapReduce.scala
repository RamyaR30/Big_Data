package org.bigdata.tutorial

import java.io.FileWriter
import scala.collection.mutable
import scala.util.Random

object Assignment1 {

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
    val idxs: Seq[Int] = (0 until size)
    val ourStream: Seq[String] = idxs.map(x => generator(x, n))

    //  val counter: mutable.Map[String, Int] = mutable.Map[String, Int]()
    //    for (word <- ourStream) {
    //      val count = counter.getOrElse(word, 0)
    //      counter.update(word,count + 1)
    //    }
    //
    //    println(counter)

    val wordCountTuples: Seq[Map[String, Int]] = ourStream.map(word => Map((word, 1)))

    val counter: Map[String, Int] = wordCountTuples.reduce(op)

    println(counter)

    //println(generator(0,10))

    // Hint: This is how to initialize and update an empty mutable Map (be aware of "import scala.collection.mutable"):
    // val ourMutableMap: mutable.Map[String, Int] = mutable.Map[String, Int]()
    // ourMutableMap.update("word_1",1)
    // ourMutableMap.update("word_2",4)

    // Hint: This is how to initialize and update an immutable Map:
    // var ourImmutableMap: Map[String, Int] = Map(("word_1",1))
    // ourImmutableMap = ourImmutableMap.updated("word_2",4)

    // Hint: This is basic code to dump to a csv file.
    // val fw = new FileWriter("output.csv")
    // for ((word, count) <- ourMap){
    //   fw.write(word + "," + count + "\n")
    // }
    // fw.flush()
    // fw.close()

   

  }

}
