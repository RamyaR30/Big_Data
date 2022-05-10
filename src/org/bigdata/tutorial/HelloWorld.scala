
package org.bigdata.tutorial

import scala.collection.mutable

object HelloWorld {

  def add1(x: Int): Int = x + 1
  def main(args: Array[String]): Unit = {
    //map function
    //val x = Map(("word1", 4),("word2", 3))
    //val xi = x.updated("word1", 10)
   // println(xi)
    //println(x)
    val x = mutable.Map(("word1", 4),("word2", 3))
    x.update("word1", 10)
    println(x)
    //MAP
    //val x: Seq[Int] = Seq(1, 2, 5, 6, 7)
   // val xi: Seq[Int] = x.map(add1)
    //println(x)
   // println(xi)
    //REDUCE
    //val y: Seq[Int] = Seq(1, 2, 5, 6, 7)
   // val yi = y.reduce((l,r)=> l+r)
   // println(y)
    //println(yi)

  }
}