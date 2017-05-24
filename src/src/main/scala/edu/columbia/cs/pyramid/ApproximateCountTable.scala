package edu.columbia.cs.pyramid

import java.io.FileOutputStream
import java.nio.ByteBuffer
import java.util.concurrent.locks.ReentrantReadWriteLock

import scala.collection.mutable
import scala.util.Random
import scala.util.hashing.MurmurHash3

/**
  * Counter function implemented using a CountMin Sketch. Will return the
  * correct count within a factor based on `eps` with probability `delta`.
  *
  * @param cmsEpsilon
  * @param cmsDelta
  */
class ApproximateCountTable(override val cmsEpsilon: Double,
                            override val cmsDelta: Double,
                            override val isBackingNoise: Boolean,
                            override val noiseLaplaceB: Option[Double],
                            override val sumNNoiseDraws: Int = 1,
                            override val seeds: Option[Array[Int]] = None)
  extends CountTable(cmsEpsilon, cmsDelta, isBackingNoise, noiseLaplaceB,
    sumNNoiseDraws) {

  validateEpsilon()
  validateDelta()

  val readWriteLock = new ReentrantReadWriteLock()

  println("epsilon: " + cmsEpsilon)
  println("delta: " + cmsDelta)
  println("noiseLaplaceB: " + noiseLaplaceB)
  println("sumNNoiseDraws: " + sumNNoiseDraws)
  val width: Int = CountTable.getWidth(cmsEpsilon)
  val depth: Int = CountTable.getDepth(cmsDelta)

  private val noiseArray: Option[Array[Array[Double]]] = (isBackingNoise, noiseLaplaceB) match {
    case (true, Some(b)) => Some(Array.fill[Double](depth, width) {
      (1 to sumNNoiseDraws).map { x => laplace(b) }.sum
    })
    case _ => None
  }

  println("table width: " + width + " and depth: " + depth)
  noiseLaplaceB match {
    case Some(b) =>
      println("noise b: " + b)
      println("noise sample: " + (1 to 5).map { x => laplace(b) }.mkString(", "))
      println("sumed noise sample: " + (1 to 5).map {
        x => (1 to sumNNoiseDraws).map { x => laplace(b) }.sum
      }.mkString(", "))
    case _ => "coucou"
  }

  private val sketchArray: Array[Array[Double]] = Array.fill[Double](depth, width) {
    (isBackingNoise, noiseLaplaceB) match {
      case (false, Some(b)) => (1 to sumNNoiseDraws).map { x => laplace(b) }.sum
      case _ => 0.0
    }
  }
  private val hashSeeds: Array[Int] = seeds match {
    case Some(s) => s
    case None => initializeHashSeeds(depth)
  }

  override def getHashSeeds(): Array[Int] = {
    return hashSeeds.clone()
  }

  protected def copySketchArray(): Array[Array[Double]] = {
    return sketchArray.map(a => a.clone)
  }

  protected def copyNoiseArray(): Array[Array[Double]] = {
    return noiseArray match {
      case Some(na) => na.map(a => a.clone)
      case None => Array.fill[Double](depth, width) {
        0.0
      }
    }
  }

  // Total used for sampling probability.
  private var total = 0.0


  /**
    * Returns an array of size `length` containing distinct random numbers.
    *
    * @param length
    * @return
    */
  private def initializeHashSeeds(length: Int): Array[Int] = {
    return (0 until length).toArray
    /*
    val random = new Random()
    val seedSet: mutable.HashSet[Int] = new mutable.HashSet[Int]()

    val hashSeeds = Array.fill[Int](length) {
      var seed: Int = random.nextInt()
      while (seedSet.contains(seed)) {
        seed = random.nextInt()
      }
      seed
    }

    return hashSeeds
    */
  }

  /**
    * Helper function to allow read access to sketchArray contents.
    *
    * @param depthIndex
    * @param widthIndex
    * @return
    */
  def get(depthIndex: Int, widthIndex: Int): Double = {
    return sketchArray(depthIndex)(widthIndex)
  }

  /**
    * Returns the index into the row for `key`. Ensures that the value is
    * positive by bit-wise anding with the int max value and moding by the width
    *
    * of the CountMinSketch.
    *
    * @param key
    * @param seed
    * @return
    */
  def getWidthIndex(key: Array[Double], seed: Int): Int = {
    val hash = MurmurHash3.arrayHash(key, seed)
    return (hash & Int.MaxValue) % width
  }

  /**
    * Increments the count for `key`.
    *
    * @param key
    */
  override def increment(key: Array[Double], by: Double): Unit = {
    readWriteLock.writeLock().lock()
    try {
      total += by
      (0 until depth).foreach { depthIndex =>
        val widthIndex = getWidthIndex(key, hashSeeds(depthIndex))
        sketchArray(depthIndex)(widthIndex) =
          sketchArray(depthIndex)(widthIndex) + by
      }
    } finally {
      readWriteLock.writeLock().unlock()
    }
  }

  /**
    * Increments the specific row and column in the sketch array.
    *
    * @param depthIndex
    * @param widthIndex
    * @param by
    */
  override def increment(depthIndex: Int, widthIndex: Int, by: Double): Unit = {
    readWriteLock.writeLock().lock()
    try {
      sketchArray(depthIndex)(widthIndex) =
        sketchArray(depthIndex)(widthIndex) + by
    } finally {
      readWriteLock.writeLock().unlock()
    }
  }

  /**
    * Returns the noise initialized in the backing table or throws an exception
    * if it is not initialized.
    *
    * @param depth
    * @param width
    * @param addBackingTableNoise
    * @return
    */
  private def getNoise(depth: Int, width: Int, addBackingTableNoise: Boolean): Double = {
    if (addBackingTableNoise) {
      return (noiseArray, isBackingNoise) match {
        case (Some(na), true) => na(depth)(width)
        case (None, true) => 0.0 // Maybe we should match on Some(0).
        case (Some(na), false) => throw new IllegalArgumentException(
          "Should not have backing noise sketch without backingNoiseEpsilon" +
            " parameter.")
        case (None, false) => throw new IllegalArgumentException(
          "Cannot add backing noise table with given constructor parameters.")
      }
    }
    return 0.0
  }

  /**
    * Queries the count table for the count of `key`.
    *
    * @param key
    * @return
    */
  override def pointQuery(key: Array[Double],
                          addBackingTableNoise: Boolean):
  Double = {
    readWriteLock.readLock().lock()
    try {
      var result: Double = Double.PositiveInfinity
      var depthIndex = 0
      while (depthIndex < depth) {
        val widthIndex: Int = getWidthIndex(key, hashSeeds(depthIndex))
        val x = sketchArray(depthIndex)(widthIndex) +
          getNoise(depthIndex, widthIndex, addBackingTableNoise)
        if (x < result) {
          result = x
        }
        depthIndex += 1
      }
      val noisyResult = result
      if (noisyResult > 0) {
        return noisyResult
      }
      else {
        return 0
      }
    } finally {
      readWriteLock.readLock().unlock()
    }
  }

  /**
    * Directly queries the count table.
    *
    * @param depthIndex
    * @param widthIndex
    * @param addBackingNoise
    * @return
    */
  override def pointQuery(depthIndex: Int, widthIndex: Int,
                          addBackingNoise: Boolean): Double = {
    readWriteLock.readLock().lock()
    try {
      return sketchArray(depthIndex)(widthIndex) + getNoise(depthIndex,
        widthIndex, addBackingNoise)
    } finally {
      readWriteLock.readLock().unlock()
    }
  }

  override def cleanup(): Unit = {
  }

  private val bytesPerDouble = 8

  private def doubleArrayToByteArray(doubleArray: Array[Double]): Array[Byte] = {
    val returnByteArray = new Array[Byte](bytesPerDouble * doubleArray.length)
    val valueBuffer = new Array[Byte](bytesPerDouble)

    for (i <- (0 until doubleArray.length)) {
      val d = doubleArray(i)
      val startIndex = i * bytesPerDouble
      ByteBuffer.wrap(valueBuffer).putDouble(d)
      for (j <- (0 until bytesPerDouble)) {
        val srcIndex = j
        val dstIndex = j + startIndex
        returnByteArray(dstIndex) = valueBuffer(srcIndex)
      }
    }
    return returnByteArray
  }

  private def byteArrayToDoubleArray(byteArray: Array[Byte]): Array[Double] = {
    require(byteArray.length % bytesPerDouble == 0)

    val returnBuffer = new Array[Double](byteArray.length % bytesPerDouble)
    val valueBuffer = new Array[Byte](bytesPerDouble)
    for (i <- (0 until returnBuffer.length)) {

      for (j <- (0 until valueBuffer.length)) {
        val srcIndex = i * bytesPerDouble + j
        val dstIndex = j
        valueBuffer(dstIndex) = byteArray(srcIndex)
      }
      val d = ByteBuffer.wrap(valueBuffer).getDouble()
      returnBuffer(i) = d
    }

    return returnBuffer
  }

  /**
    * Serializes the count table to the outputStream. Writes epsilon, double and
    * then the sketch in row major order.
    *
    * @param outputStream
    */
  override def serialize(outputStream: FileOutputStream): Unit = {
    val configArray = Array(cmsEpsilon, cmsDelta)
    val serializedConfigArray = doubleArrayToByteArray(configArray)
    outputStream.write(serializedConfigArray)
    for (d <- (0 until depth)) {
      outputStream.write(doubleArrayToByteArray(sketchArray(d)))
    }
  }


}
