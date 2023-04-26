package org.bigdata.tutorials

import java.io.FileWriter
import scala.collection.mutable
import scala.util.Random

object MyDistributedKVStore2Solution {

  // A random number generator.
  var random = new Random(1234)

  // Number of different keys and values.
  var nkeys = 100
  var nvalues = 100

  // Fail rate for machines.
  var failRate = 0.001

  // Rate of read access.
  var readRate = 0.85

  // Number of machines.
  var nmachines = 42

  // Simulation time.
  var time = 0

  // Number of modifications.
  var nmodifications = 10000

  // Fake stores.
  var stores: Array[mutable.Map[String, String]] = (0 until nmachines).map(_ => mutable.Map[String, String]()).toArray

  // To track read-write performance.
  var readWrites: mutable.Map[(Int, Int), Int] = mutable.Map[(Int, Int), Int]()

  def simulateGet(key: String, machine: Int): Option[String] = {
    readWrites.update((machine, time), readWrites.getOrElse((machine, time), 0) + 1)
    stores(machine).get(key)
  }

  def simulateSet(key: String, value: String, machine: Int): Unit = {
    readWrites.update((machine, time), readWrites.getOrElse((machine, time), 0) + 1)
    stores(machine).put(key, value)
  }

  // Simulation: Run a simulation step.
  def simulate(): Unit = {
    time = time + 1
    for (machine <- 0 until nmachines) {
      if (random.nextDouble() < failRate) {
        // Full drop-out of this machine.
        stores(machine).clear()
      }
    }
  }

  def resetSimulation(seed: Long = 1234): Unit = {
    random = new Random(seed)
    time = 0
    stores = (0 until nmachines).map(_ => mutable.Map[String, String]()).toArray
    readWrites = mutable.Map[(Int, Int), Int]()
  }

  var replication = 4

  // Hash key to location (based on hash) but giving more
  // than a single location to replicate.
  def location(key: String): Seq[Int] = {
    val start = Math.abs(key.hashCode) % nmachines

    (start until (start + Math.min(replication, nmachines))).map {
      case x if x < nmachines => x
      case x => x - nmachines // Start again on first machine.
    }
  }

  def get(key: String): Option[String] = {
    for (machine <- location(key)) {
      val option = simulateGet(key, machine)

      if (option.isDefined) return option
    }
    // We dont know it.
    None
  }

  def set(key: String, value: String): Unit = {
    for (machine <- location(key)) {
      simulateSet(key, value, machine)
    }
  }

  def avg(xs: Seq[Double]): Double = xs.sum / xs.length.toDouble

  // Example experiments:
  def main(args: Array[String]): Unit = {
    val fw = new FileWriter("data/simulationKV.csv")

    fw.write("failRate,replication,inconsistentPercent,missingPercent,avgWriteRead\n")

    for (parameter1 <- 1 to nmachines) {
      for (parameter2 <- 1 to 40) {
        failRate = 0.00025 * parameter2
        println("-----------------------")
        replication = parameter1
        println("replication: " + replication)

        resetSimulation()

        // Do experiment.
        val (inconsistent, missing) = experiment()

        // Store stuff.
        val inconsistentPercent = (inconsistent * 100f / nmodifications)
        val missingPercent = (missing * 100f / nmodifications)
        val avgWriteRead = avg(getReadWrites().values.map(_.toDouble).toSeq)

//        println("Inconsistent reads: " + inconsistent + " (" + inconsistentPercent.toInt + "%)")
//        println("Missing reads: " + missing + " (" + missingPercent + "%)")
//        println(avgWriteRead)

        fw.write(failRate + "," + replication + "," + inconsistentPercent + "," + missingPercent + "," + avgWriteRead + "\n")
      }
    }
    fw.flush()
    fw.close()
  }

  def experiment(): (Int, Int) = {

    // State used to test implementation.
    val state = mutable.Map[String, String]()

    var inconsistent = 0
    var missing = 0

    for (modification <- 0 until nmodifications) { // Modification steps.

      // Decide between reading or writing.
      if (random.nextDouble() < readRate && state.nonEmpty) {

        // Pick a key already assigned.
        val key = random.shuffle(state.keys.toSeq).head
        val result = get(key)

        // Read the key and check if distributed storage is consistent with state.
        if (result.isEmpty) missing = missing + 1
        else if (state(key) != result.get) inconsistent = inconsistent + 1
      }
      else {

        // Pick a random new or old key.
        val key = "key_" + random.nextInt(nkeys)
        val value = "value_" + random.nextInt(nvalues)

        // Assign to random value.
        state.update(key, value)
        set(key, value)
      }

      simulate() // Simulate next time step and random drop-out.
    }

    // TODO: Feel free to dump stuff to a csv.
    //    println("Inconsistent reads: " + inconsistent + " (" + (inconsistent * 100 / nmodifications) + "%)")
    //    println("Missing reads: " + missing + " (" + (missing * 100 / nmodifications) + "%)")
    //
    //    plotReadWrites()
    (inconsistent, missing)
  }

  // Helper to use 'reduceByKey'.
  implicit class BetterIterable[K, V](val xs: Iterable[(K, V)]) {
    def reduceByKey(op: (V, V) => V): Map[K, V] = xs.groupMapReduce(x => x._1)(x => x._2)(op)
  }

  def getReadWrites(step: Int = 2500): Map[Int, Int] = {
    val sumByMachineStep: Map[(Int, Int), Int] = readWrites.toSeq
      .map { case ((machine, time), count) => ((machine, time / step), count) }
      .reduceByKey((l, r) => l + r)

    val maxByStep = sumByMachineStep.toSeq
      .map { case ((machine, time), count) => (time, count) }
      .reduceByKey((l, r) => Math.max(l, r))

    maxByStep
  }

  // This can be used to present nice output.
  def plotReadWrites(step: Int = 2500): Unit = {
    for ((s, count) <- getReadWrites(step)) {
      println("For time [" + s * step + "," + (s + 1) * step + "] max rw is " + count)
    }
  }


}
