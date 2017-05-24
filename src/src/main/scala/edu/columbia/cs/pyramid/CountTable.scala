package edu.columbia.cs.pyramid

import java.io.{FileOutputStream}

import scala.util.Random
import scala.reflect.ClassTag


object CountTable {

  def getWidth(cmsEpsilon: Double): Int = {
    return math.ceil(math.E / cmsEpsilon).toInt
  }

  def getDepth(cmsDelta: Double): Int = {
    return math.ceil(math.log(1 / cmsDelta)).toInt
  }

}

/**
  * Generic counter trait for counting features in Pyramid.
  *
  */
abstract class CountTable(val cmsEpsilon: Double,
                          val cmsDelta: Double,
                          val isBackingNoise: Boolean,
                          val noiseLaplaceB: Option[Double],
                          val sumNNoiseDraws: Int = 1,
                          val seeds: Option[Array[Int]] = None)
  extends Serializable {

  private val generator = new Random()

  private def nextFloat(): Double = {
    var r = generator.nextFloat()
    while (r == 1.0 || r == 0.0) r = generator.nextFloat()
    return r
  }

  /**
    * Generates a random value picked from a laplacican distribution.
    *
    * @return
    */
  def laplace(laplaceB: Double): Double = {
    if (laplaceB == 0) { return 0.0 }

    val p = nextFloat()
    if (p < 0.5) {
      return laplaceB * math.log(2.0 * p)
    }
    else {
      return -1 * laplaceB * math.log(2.0 * (1 - p))
    }
  }

  /**
    * Samples from a laplacian distribution using epsilon as sd = 1 / e or
    * returning 0 if None.
    *
    * @return
    */
  def laplace(laplaceB: Option[Double]): Double = laplaceB match {
    case Some(b) => laplace(b)
    case None => 0.0
  }

  /**
    * Increments the count for `key`.
    *
    * @param key
    */
  def increment(key: Array[Double], by: Double): Unit

  /**
    * Increments the key by one.
    *
    * @param key
    */
  def increment(key: Array[Double]): Unit = {
    increment(key)
  }

  /**
    * Increments the specific row and column in the sketch array.
    *
    * @param depthIndex
    * @param widthIndex
    * @param by
    */
  def increment(depthIndex: Int, widthIndex: Int, by: Double): Unit

  /**
    * Increments the value at the depth and width by 1.
    *
    * @param depthIndex
    * @param widthIndex
    */
  def increment(depthIndex: Int, widthIndex: Int): Unit = {
    increment(depthIndex, widthIndex, 1)
  }

  /**
    * Queries the count table for the count of `key`.
    *
    * @param key
    * @return
    */
  def pointQuery(key: Array[Double], addBackingNoise: Boolean): Double

  final def pointQuery(key: Array[Double]): Double = {
    return pointQuery(key, false)
  }

  /**
    * Directly queries the count table.
    *
    * @param depthIndex
    * @param widthIndex
    * @param addBackingNoise
    * @return
    */
  def pointQuery(depthIndex: Int, widthIndex: Int,
                 addBackingNoise: Boolean): Double

  final def pointQuery(depthIndex: Int, widthIndex: Int): Double = {
    return pointQuery(depthIndex, widthIndex, false)
  }

  /**
    * Perform any needed cleanup.
    */
  def cleanup(): Unit

  /**
    * Serializes the count table to the outputStream.
    *
    * @param outputStream
    */
  def serialize(outputStream: FileOutputStream)

  /**
    * Merges the data from that count table into this count table.
    *
    * @param that
    */
  def merge(that: CountTable): Unit = {
    require(ClassTag(this.getClass) == ClassTag(that.getClass))
    require(that.cmsDelta == cmsDelta)
    require(that.cmsEpsilon == cmsEpsilon)
    // require(that.backingNoiseEpsilon == backingNoiseEpsilon)
    // require(that.initNoiseEpsilon == initNoiseEpsilon)
    val thisHashSeeds = getHashSeeds()
    val thatHashSeeds = that.getHashSeeds()
    require(thisHashSeeds.length == thatHashSeeds.length)
    require(thisHashSeeds.length > 0)
    require(thatHashSeeds.length > 0)
    for (i <- 0 until thisHashSeeds.length) {
      require(thisHashSeeds(i) == thatHashSeeds(i))
    }

    // We expect the width the be relatively large.
    (0 until CountTable.getWidth(cmsEpsilon)).par.foreach { w =>
      (0 until CountTable.getDepth(cmsDelta)).foreach { d =>
        increment(d, w, that.pointQuery(d, w))
      }
    }
  }


  /**
    * Validates that epsilon fits the following conditions:
    * - is > 0
    */
  final def validateEpsilon(): Unit = {

    if (cmsEpsilon <= 0) {
      throw new IllegalArgumentException("eps must be greater than 0.")
    }
  }

  /**
    * Validates that delta fits the following conditions:
    * - is > 0
    */
  final def validateDelta(): Unit = {
    if (cmsDelta <= 0) {
      throw new IllegalArgumentException("delta must be greater than 0.")
    }
  }

  /**
    *
    * @return Returns the values used as seeds to the hash function for each row.
    */
  def getHashSeeds(): Array[Int]

  /**
    * Returns a positive modulus of i % j.
    */
  def positiveMod(i: Int, m: Int): Int = {
    return (i & Int.MaxValue) % m
  }
}
