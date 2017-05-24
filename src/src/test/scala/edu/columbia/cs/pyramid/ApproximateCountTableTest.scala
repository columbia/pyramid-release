package edu.columbia.cs.pyramid

import org.scalatest.{FunSuite, Matchers}

import scala.collection.mutable.ArrayBuffer

class ApproximateCountTableTest extends FunSuite with Matchers {

  val cmsEpsilon = 0.00001
  val cmsDelta = 0.00001

  val isBackingNoise: Boolean = false
  val noiseLaplaceB: Option[Double] = None

  def getMean(values: Seq[Double]): Double = {
    require(values.length > 0)
    return values.sum / values.length
  }

  def getSD(values: Seq[Double]): Double = {
    val mean = getMean(values)
    val s = values.map { v =>
      math.pow((v - mean), 2)
    }.sum
    return math.sqrt(s / values.length)
  }

  test("CountTable laplace should have mean around 0 and sd around 100") {
    val lapSD = 10.0
    val e = 1.0 / lapSD
    val cms = new ApproximateCountTable(cmsEpsilon, cmsDelta,
      isBackingNoise, noiseLaplaceB)
    val draws = 1000000
    val laplaces = (0 until draws).map(i => cms.laplace(e))
    val mean = getMean(laplaces)
    val sd = getSD(laplaces)

    mean should equal(0.0 +- 1.0)
    // math.abs(sd) should equal (lapSD +- 1.0)

  }

  test("CountMinSketch elements should be initialized to 0") {
    val cms = new ApproximateCountTable(cmsEpsilon, cmsDelta,
      isBackingNoise, noiseLaplaceB)
    (0 to (cms.width - 1)).foreach { widthIndex =>
      (0 to (cms.depth - 1)).foreach { depthIndex =>
        cms.get(depthIndex, widthIndex) should equal(0)
      }
    }
  }

  test("CountMinSketch elements should have mean around 0 with sd 10") {
    val lapSD = 10.0
    val initNoiseEpsilon = Some(1.0 / lapSD)
    val cms = new ApproximateCountTable(cmsEpsilon, cmsDelta,
      isBackingNoise, initNoiseEpsilon)

    val elements = ArrayBuffer.empty[Double]
    (0 to (cms.width - 1)).foreach { widthIndex =>
      (0 to (cms.depth - 1)).foreach { depthIndex =>
        elements += cms.get(depthIndex, widthIndex)
      }
    }
    elements.length should equal(cms.width * cms.depth)
    val mean = getMean(elements)
    mean should equal(0.0 +- 0.1)
  }

  test("getWidthIndex should always be positive and smaller than the width") {
    val seed = 13 // I have no reason for picking this seed.
    val cms = new ApproximateCountTable(cmsEpsilon, cmsDelta,
        isBackingNoise, noiseLaplaceB)

    (0 to 250000).foreach { i =>
      val widthIndex = cms.getWidthIndex(Array.fill[Double](1) {
        i.toDouble
      },
        seed)
      widthIndex should be >= 0
      widthIndex should be < cms.width
    }
  }

  test("Merging two count tables should result in the combination of the two") {
    val k1 = Array(22.0)
    val k2 = Array(1.0, 2.0)
    val k3 = Array(5.0, 3.0)
    val cms1Inc = 1.0
    val cms2Inc = 5.0
    val cms1 = new ApproximateCountTable(cmsEpsilon, cmsDelta,
      isBackingNoise, noiseLaplaceB)
    val cms2 = new ApproximateCountTable(cmsEpsilon, cmsDelta,
      isBackingNoise, noiseLaplaceB, seeds = Some(cms1.getHashSeeds()))

    cms1.getHashSeeds.toSeq should equal(cms2.getHashSeeds.toSeq)

    cms1.increment(k1, cms1Inc)
    cms1.increment(k2, cms1Inc)
    cms1.increment(k3, cms1Inc)
    cms1.pointQuery(k1) should be >= cms1Inc
    cms1.pointQuery(k2) should be >= cms1Inc
    cms1.pointQuery(k3) should be >= cms1Inc

    cms2.increment(k1, cms2Inc)
    cms2.increment(k2, cms2Inc)
    cms2.increment(k3, cms2Inc)
    cms2.pointQuery(k1) should be >= cms2Inc
    cms2.pointQuery(k2) should be >= cms2Inc
    cms2.pointQuery(k3) should be >= cms2Inc

    cms1.merge(cms2)
    cms1.pointQuery(k1) should be >= (cms1Inc + cms2Inc)
    cms1.pointQuery(k2) should be >= (cms1Inc + cms2Inc)
    cms1.pointQuery(k3) should be >= (cms1Inc + cms2Inc)
  }

  test("The addBackingNoise parameter does actually control.") {
    val k1 = Array(22.0)
    val k2 = Array(1.0, 2.0)
    val k3 = Array(5.0, 3.0)
    val incrBy = 1000.0
    val isBackingNoiseEpsilon = true
    val localLaplaceB: Option[Double] = Some(1000.0)
    val cms = new ApproximateCountTable(cmsEpsilon, cmsDelta,
      true, localLaplaceB)

    // Should be 0 initially without
    cms.pointQuery(k1, false) should be(0.0)
    cms.pointQuery(k2, false) should be(0.0)
    cms.pointQuery(k3, false) should be(0.0)

    cms.increment(k1, incrBy)
    cms.increment(k2, incrBy)
    cms.increment(k3, incrBy)

    // We expect it to be less than incrBy but I don't want to assert that.
    cms.pointQuery(k1, true) should not be (incrBy)
    cms.pointQuery(k2, true) should not be (incrBy)
    cms.pointQuery(k3, true) should not be (incrBy)
  }

}