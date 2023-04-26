package org.bigdata.tutorial


object TasksSolution {

  def main(args: Array[String]): Unit = {

    // The following are simple test cases to check you implementation.
    // Task 1:
    assert(task1(Seq(1, 2, 3)) == Seq("odd", "even", "odd"))

    // Task 2:
    assert(task2(Seq("ClassA.scala", "ClassB.java", "readme.txt")) == Seq("ClassB.java"))

    // Task 3:
    assert(task3(Seq(1, 2, 3)) == 1 * 2 * 3)

    // Task 4:
    assert(task4(Seq("a", "b", "c")) == "abc")

    // Task 5:
    assert(task5(Seq("m1", "m2", "m3"), "m1"))
    assert(!task5(Seq("m1", "m2", "m3"), "m4"))

    // Task 6:
    assert(task6(Seq(("k1", "v1"), ("k1", "v2"), ("k2", "v1"))) == Map("k1" -> Seq("v1", "v2"), "k2" -> Seq("v1")))

    // Task 7:
    val l = Seq(("k1", "v1"), ("k2", "v2"))
    val r = Seq(("k2", "vv2"), ("k3", "vv2"))
    assert(task7(l, r) == Seq(("k2", "v2", "vv2")))
  }

  /**
   * Return a list of strings "even" and "odd" for the input sequence of integers.
   */
  def task1(seq: Seq[Int]): Seq[String] = seq.map {
    case x if x % 2 == 0 => "even"
    case _ => "odd"
  }

  /**
   * Filter a list of path names for ".java" files.
   */
  def task2(seq: Seq[String]): Seq[String] =
    seq.filter(x => x.split("\\.").last == "java")

  /**
   * Form the product of all numbers contained in the collection.
   */
  def task3(seq: Seq[Int]): Int = seq.product

  /**
   * Concat the individual strings of this collection forming a single string.
   */
  def task4(seq: Seq[String]): String =
    seq.reduceOption((l, r) => l + r).getOrElse("")

  /**
   * Check if x is a member (contained) in the sequence.
   */
  def task5(seq: Seq[String], search: String): Boolean =
    seq.foldLeft(false)((b, element) => b || element == search)

  /**
   * (Optional) Implement grouping by a key, without using the "groupBy" method provided on the collection,
   * but the fold method.
   * The first entry of the tuples is the key that you should use for grouping.
   * Optional: This is an advanced assignment, you may get additional points.
   */
  def task6(seq: Seq[(String, String)]): Map[String, Seq[String]] = {
    val intermediate: Seq[Map[String, Seq[String]]] = seq.map { case (key, value) => Map(key -> Seq(value)) }

    def op(l: Map[String, Seq[String]], r: Map[String, Seq[String]]): Map[String, Seq[String]] =
      (l.keySet ++ r.keySet).map(key => key -> (l.getOrElse(key, Seq()) ++ r.getOrElse(key, Seq()))).toMap

    intermediate.reduce((l, r) => op(l, r))
  }

  /**
   * (Optional) Join the two sequences by the key (first entry of the tuple). Produce a 3-tuple containing
   * the results (key, left, right).
   * Optional: This is an advanced assignment, you may get additional points.
   */
  def task7(l: Seq[(String, String)], r: Seq[(String, String)]): Seq[(String, String, String)] = {
    // This shows how to implement a join using groupBy.
    val ll: Seq[(String, (Option[String], Option[String]))] = l
      .map { case (key, value) => (key, (Some(value), None)) }

    val rr: Seq[(String, (Option[String], Option[String]))] = r
      .map { case (key, value) => (key, (None, Some(value))) }

    val cc: Seq[(String, (Option[String], Option[String]))] = ll ++ rr

    val grouped: Map[String, Seq[(String, (Option[String], Option[String]))]] = cc
      .groupBy { case (key, _) => key }

    grouped.flatMap { case (key, seq) =>
      // Caution, different collect method (Scala)!
      val leftValues = seq.collect { case (_, (Some(value), None)) => value }
      val rightValues = seq.collect { case (_, (None, Some(value))) => value }

      // Cartesian product.
      leftValues.flatMap(left => rightValues.map(right => (key, left, right)))
    }.toSeq
  }

}
