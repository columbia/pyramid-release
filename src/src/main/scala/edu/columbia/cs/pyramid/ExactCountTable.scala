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
class ExactCountTable(override val isBackingNoise: Boolean,
                      override val noiseLaplaceB: Option[Double],
                      override val sumNNoiseDraws: Int = 1)
  extends CountTable(0.0, 0.0, isBackingNoise, noiseLaplaceB, sumNNoiseDraws) {

  val readWriteLock = new ReentrantReadWriteLock()

  println("noiseLaplaceB: " + noiseLaplaceB)
  println("sumNNoiseDraws: " + sumNNoiseDraws)

  private val noiseTable: Option[mutable.HashMap[String, Double]] =
  (isBackingNoise, noiseLaplaceB) match {
    case (true, Some(b)) => Some(mutable.HashMap[String, Double]())
    case _ => None
  }

  noiseLaplaceB match {
    case Some(b) =>
      println("noise b: " + b)
      println("noise sample: " + (1 to 5).map { x => laplace(b) }.mkString(", "))
      println("sumed noise sample: " + (1 to 5).map {
        x => (1 to sumNNoiseDraws).map { x => laplace(b) }.sum
      }.mkString(", "))
    case _ => "coucou"
  }

  private val countTable: mutable.HashMap[String, Double] =
  mutable.HashMap[String, Double]()

  // Total used for sampling probability.
  private var total = 0.0

  /**
    * Increments the count for `key`.
    *
    * @param key
    */
  override def increment(key: Array[Double], by: Double): Unit = {
    readWriteLock.writeLock().lock()
    try {
      total += by
      val keyString = key.mkString(",")
      if (!countTable.contains(keyString)) {
        val noise: Double = noiseLaplaceB match {
          case Some(b) => (1 to sumNNoiseDraws).map { x => laplace(b) }.sum
          case _ => 0.0
        }
        if (isBackingNoise) {
          noiseTable match {
            case Some(nt) => nt += (keyString -> noise)
            case None     => ()
          }
          countTable += (keyString -> by)
        } else {
          countTable += (keyString -> (by + noise))
        }
      } else {
        countTable += (keyString -> (countTable(keyString) + by))
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
    throw new IllegalArgumentException("Tried to increment cell of ExactCountTable")
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
  private def getNoise(key: Array[Double], addBackingTableNoise: Boolean): Double = {
    if (addBackingTableNoise) {
      val keyString = key.mkString(",")
      return (noiseTable, isBackingNoise) match {
        case (Some(nt), true) => nt.getOrElse(keyString, 0.0)
        case (None, true) => 0.0 // Maybe we should match on Some(0).
        case (Some(nt), false) => throw new IllegalArgumentException(
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
      val keyString = key.mkString(",")
      val noisyResult = countTable.getOrElse(keyString, 0.0) +
                        getNoise(key, addBackingTableNoise)
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
    throw new IllegalArgumentException("Cannot call pointQuery with depth/width on exact table.")
  }

  override def cleanup(): Unit = {
  }

  override def getHashSeeds(): Array[Int] = {
    return Array[Int]()
  }

  // NOTE: I think this is legacy and is not needed
  /**
    * Serializes the count table to the outputStream. Writes epsilon, double and
    * then the sketch in row major order.
    *
    * @param outputStream
    */
  override def serialize(outputStream: FileOutputStream): Unit = {
  }

  /**
    * Merges the data from that count table into this count table.
    *
    * @param that
    */
  def merge(that: ExactCountTable): Unit = {
    // TODO
  }
}
