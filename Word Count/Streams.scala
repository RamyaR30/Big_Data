package org.bigdata.tutorials

import scala.collection.mutable
import scala.io.Source

object MySteamSoluiton {

  def main(args: Array[String]): Unit = {

    // Our stream coming from the frank.txt file.
    val source = Source.fromFile("data/frank.txt")
    for (paragraph <- source.getLines) {
      // Splitting the paragraph into words.
      val words = paragraph.replaceAll("[^A-Za-z0-9]", " ").split(" ")
      // Passing the words to the consumers of this stream.
      for (word <- words if word != "") {
        consume1(word)
        consume2(word)
        consume3(word)
        consume4(word)
        consume5(word)
        consume6(word)
      }
    }
  }

  // Example (Task 1) --------------------------------------
  // TAG: This is the final total count for consumer method 1 (stored in variable totalCount).
  var totalCount = 0

  def consume1(word: String): Unit = {
    // TODO: Compute and store the number of total words in the text.
    totalCount = totalCount + 1

    // println(totalCount)
  }

  // Task 2 -----------------------------------------------
  // TAG: Use store2.size to get the result.
  val store2: mutable.Set[String] = mutable.Set[String]()

  def consume2(word: String): Unit = {
    // TODO: Compute the number of distinct words in the text.
    store2.add(word)

    //println(store2.size)
  }

  // Task 3 -----------------------------------------------
  // TAG: This is the result.
  val store3: mutable.Map[String, Int] = mutable.Map[String, Int]()

  def consume3(word: String): Unit = {
    // TODO: Compute and store the word-counts in the text.
    store3.put(word, store3.getOrElse(word, 0) + 1)

    //println(store3.get("the))
  }

  // Task 4 -----------------------------------------------

  // TAG: This is the result after the first element of the stream has been computed.
  var store4 = ""

  def consume4(word: String): Unit = {
    // TODO: Compute and store the longest word in the text.
    if (word.length > store4.length) store4 = word

    //println(store4)
  }

  // Task 5 -----------------------------------------------
  // TAG: The word that appears most often can be recovered based on store5b.last.
  val store5a: mutable.Map[String, Int] = scala.collection.mutable.Map[String, Int]()
  val store5b: mutable.Set[(Int, String)] = scala.collection.mutable.SortedSet[(Int, String)]()

  def consume5(word: String): Unit = {
    // TODO: Compute and store the word that appears most often.

    // Recover if previously there.
    val last: Option[Int] = store5a.remove(word)

    // Remove from sorted set if previously there.
    last.foreach(previous => store5b.remove((previous, word)))

    // Compute next.
    val next = last.getOrElse(0) + 1

    // Update stores.
    store5a.put(word, next)
    store5b.add((next, word))

    //println(store5b.last)
  }

  // Task 6 -----------------------------------------------

  var n = 0
  var sum = 0
  var mean = 0.0

  def consume6(word: String): Unit = {
    // TODO: Compute and store the mean length of the words.
    n = n + 1
    sum = sum + word.length
    mean = sum.toFloat / n
    // println(n + " " + Math.round(mean*100)/100f)
  }

}
