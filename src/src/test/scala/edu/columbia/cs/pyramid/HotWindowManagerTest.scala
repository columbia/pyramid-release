package edu.columbia.cs.pyramid

import org.scalatest.{FunSuite, Matchers}
import scala.util.Random

object HotWindowManagerTest {

  // No noise for now in the master hot window.
  val masterNoise = None

  // Just use 0 to 99 as hash seeds.
  val hashSeeds = (0 until 100).map(_ => Random.nextInt())

  val cmsEpsilon = 0.00001

  val cmsDelta = 0.00001

  val isBackingNoise = false

  val noiseLaplaceB: Option[Double] = None

  val featureCount = 3

  val labels = Seq(1.0, -1.0)

  val depFeatures = Seq(Seq(1, 2))

  val floatEpsilon: Double = 0.01

  def instantiateCMS(name: String, laplaceB: Option[Double]): ApproximateCountTable = {
    val depth = CountTable.getDepth(cmsDelta)
    val seeds = Some(hashSeeds.take(depth).toArray)
    return new ApproximateCountTable(cmsEpsilon, cmsDelta, isBackingNoise,
      laplaceB)
  }

  def instantiateLabelCounter(name: String): LocalLabelCounter = {
    return new LocalLabelCounter()
  }

  def instantiateFeaturizer(name: String): Featurizer = {
    return new Featurizer(name, labels, featureCount, depFeatures,
      instantiateCMS, instantiateLabelCounter)
  }
}

class HotWindowManagerTest extends FunSuite with Matchers {
  // Number of elements to keep in each active in the hot window.
  val hotWindowSize = 10000

  // The number of elements to be in each count table window.
  val countTableSize = 20000

  // Number of total hot windows.
  val countTableCount = 10

  val featurizeWithCounts = true
  val featurizeWithProbabilities = true
  val addBackingNoise = false

  def getPoint1(): LabeledPoint = {
    return new LabeledPoint(-1, Array(1.0, 1.0, 3.0))
  }

  def getPoint2(): LabeledPoint = {
    return new LabeledPoint(-1, Array(2.0, 2.0, 4.0))
  }

  def getPoint3(): LabeledPoint = {
    return new LabeledPoint(1, Array(1.0, 1.0, 3.0))
  }

  def getPoint4(): LabeledPoint = {
    return new LabeledPoint(1, Array(2.0, 2.0, 4.0))
  }


  def compareFeatureVectors(fv1: Array[Double], fv2: Array[Double]): Unit = {
    println("fv1:\n" + fv1.mkString(","))
    println("fv2:\n" + fv2.mkString(","))
    fv1 should have length fv2.length
    fv2 should have length fv1.length

    for (i <- 0 until fv1.length) {
      fv1(i) should be(fv2(i) +- HotWindowManagerTest.floatEpsilon)
    }
  }

  test("Featurizer and HotWindowManager should featurize points in the same " +
    "fashion") {
    val featurizer = HotWindowManagerTest.instantiateFeaturizer("TestFeaturizer")
    val lhwc = new LocalHotWindowCollection()
    val hwm = new HotWindowManager(hotWindowSize, countTableCount,
      countTableSize, None, lhwc, HotWindowManagerTest.instantiateFeaturizer,
      HotWindowManagerTest.instantiateFeaturizer)

    // Should be ~40k loops with 80k points.
    val observationLoops = 40000
    for (i <- 0 until observationLoops) {
      hwm.addObservation(getPoint1())
      hwm.addObservation(getPoint2())
      hwm.addObservation(getPoint3())
      hwm.addObservation(getPoint4())

      featurizer.addObservation(getPoint1())
      featurizer.addObservation(getPoint2())
      featurizer.addObservation(getPoint3())
      featurizer.addObservation(getPoint4())
    }

    compareFeatureVectors(hwm.featurizePoint(getPoint1(), featurizeWithCounts,
      featurizeWithProbabilities, addBackingNoise),
      featurizer.featurizePoint(getPoint1(), featurizeWithCounts,
        featurizeWithProbabilities, addBackingNoise))
    compareFeatureVectors(hwm.featurizePoint(getPoint2(), featurizeWithCounts,
      featurizeWithProbabilities, addBackingNoise),
      featurizer.featurizePoint(getPoint2(), featurizeWithCounts,
        featurizeWithProbabilities, addBackingNoise))
    compareFeatureVectors(hwm.featurizePoint(getPoint3(), featurizeWithCounts,
      featurizeWithProbabilities, addBackingNoise),
      featurizer.featurizePoint(getPoint3(), featurizeWithCounts,
        featurizeWithProbabilities, addBackingNoise))
    compareFeatureVectors(hwm.featurizePoint(getPoint4(), featurizeWithCounts,
      featurizeWithProbabilities, addBackingNoise),
      featurizer.featurizePoint(getPoint4(), featurizeWithCounts,
        featurizeWithProbabilities, addBackingNoise))
  }

  test("Hot window manager should be threadsafe and featurize points like " +
    "featurizer access serially") {
    val featurizer = HotWindowManagerTest.instantiateFeaturizer("TestFeaturizer")
    val lhwc = new LocalHotWindowCollection()
    val hwm = new HotWindowManager(hotWindowSize, countTableCount,
      countTableSize, None, lhwc, HotWindowManagerTest.instantiateFeaturizer,
      HotWindowManagerTest.instantiateFeaturizer)

    // Should be ~40k loops with 80k points.
    val observationLoops = 20000
    for (i <- 0 until observationLoops) {
      featurizer.addObservation(getPoint1())
      featurizer.addObservation(getPoint2())
      featurizer.addObservation(getPoint3())
      featurizer.addObservation(getPoint4())
    }

    val threadCount = 20
    val threads = (0 until threadCount).par
    threads.foreach { t =>
      for (i <- 0 until observationLoops / threadCount) {
        hwm.addObservation(getPoint1())
        hwm.addObservation(getPoint2())
        hwm.addObservation(getPoint3())
        hwm.addObservation(getPoint4())
      }
    }

    compareFeatureVectors(hwm.featurizePoint(getPoint1(), true, true, false),
      featurizer.featurizePoint(getPoint1(), true, true, false))
    compareFeatureVectors(hwm.featurizePoint(getPoint2(), true, true, false),
      featurizer.featurizePoint(getPoint2(), true, true, false))
    compareFeatureVectors(hwm.featurizePoint(getPoint3(), true, true, false),
      featurizer.featurizePoint(getPoint3(), true, true, false))
    compareFeatureVectors(hwm.featurizePoint(getPoint4(), true, true, false),
      featurizer.featurizePoint(getPoint4(), true, true, false))

  }

  test("Featurizer and HotWindowManager should featurize points in the same " +
    "fashion after serialization") {
    val featurizer = HotWindowManagerTest.instantiateFeaturizer("TestFeaturizer")
    val lhwc = new LocalHotWindowCollection()
    val hwm = new HotWindowManager(hotWindowSize, countTableCount,
      countTableSize, None, lhwc, HotWindowManagerTest.instantiateFeaturizer,
      HotWindowManagerTest.instantiateFeaturizer)

    // Should be ~40k loops with 80k points.
    val observationLoops = 20000
    for (i <- 0 until observationLoops) {
      hwm.addObservation(getPoint1())
      hwm.addObservation(getPoint2())
      hwm.addObservation(getPoint3())
      hwm.addObservation(getPoint4())

      featurizer.addObservation(getPoint1())
      featurizer.addObservation(getPoint2())
      featurizer.addObservation(getPoint3())
      featurizer.addObservation(getPoint4())
    }

    val outFile = "/tmp/serializedTable"
    CountTableLib.serializeHotWindowManager(hwm, outFile)
    val hwmD = CountTableLib.deserializeHotWindowManager(outFile)
    val mergedFeaturizer = hwmD.getMergedWindow(true)

    compareFeatureVectors(hwmD.featurizePoint(getPoint1(), featurizeWithCounts,
      featurizeWithProbabilities, addBackingNoise),
      featurizer.featurizePoint(getPoint1(), featurizeWithCounts,
        featurizeWithProbabilities, addBackingNoise))
    compareFeatureVectors(mergedFeaturizer.featurizePoint(getPoint1(),
      featurizeWithCounts, featurizeWithProbabilities, addBackingNoise),
      featurizer.featurizePoint(getPoint1(), featurizeWithCounts,
        featurizeWithProbabilities, addBackingNoise))

    compareFeatureVectors(hwmD.featurizePoint(getPoint2(), featurizeWithCounts,
      featurizeWithProbabilities, addBackingNoise),
      featurizer.featurizePoint(getPoint2(), featurizeWithCounts,
        featurizeWithProbabilities, addBackingNoise))
    compareFeatureVectors(mergedFeaturizer.featurizePoint(getPoint2(),
      featurizeWithCounts, featurizeWithProbabilities, addBackingNoise),
      featurizer.featurizePoint(getPoint2(), featurizeWithCounts,
        featurizeWithProbabilities, addBackingNoise))

    compareFeatureVectors(hwmD.featurizePoint(getPoint3(), featurizeWithCounts,
      featurizeWithProbabilities, addBackingNoise),
      featurizer.featurizePoint(getPoint3(), featurizeWithCounts,
        featurizeWithProbabilities, addBackingNoise))
    compareFeatureVectors(mergedFeaturizer.featurizePoint(getPoint3(),
      featurizeWithCounts, featurizeWithProbabilities, addBackingNoise),
      featurizer.featurizePoint(getPoint3(), featurizeWithCounts,
        featurizeWithProbabilities, addBackingNoise))

    compareFeatureVectors(hwmD.featurizePoint(getPoint4(), featurizeWithCounts,
      featurizeWithProbabilities, addBackingNoise),
      featurizer.featurizePoint(getPoint4(), featurizeWithCounts,
        featurizeWithProbabilities, addBackingNoise))
    compareFeatureVectors(mergedFeaturizer.featurizePoint(getPoint4(),
      featurizeWithCounts, featurizeWithProbabilities, addBackingNoise),
      featurizer.featurizePoint(getPoint4(), featurizeWithCounts,
        featurizeWithProbabilities, addBackingNoise))
  }

  def shouldAllBeNonZero(fv: Array[Double]): Unit = {
    println(fv.mkString(","))
    for (f: Double <- fv) {
      f should be > 0.0
    }
  }

  test("Test that after large observation additions counts are recorded.") {
    val lhwc = new LocalHotWindowCollection()
    val hwm = new HotWindowManager(hotWindowSize, countTableCount,
      countTableSize, None, lhwc, HotWindowManagerTest.instantiateFeaturizer,
      HotWindowManagerTest.instantiateFeaturizer)

    val observationLoops = 100000
    for (i <- 0 until observationLoops) {
      hwm.addObservation(getPoint1())
      hwm.addObservation(getPoint2())
      hwm.addObservation(getPoint3())
      hwm.addObservation(getPoint4())
    }

    val outFile = "/tmp/serializedTable"
    CountTableLib.serializeHotWindowManager(hwm, outFile)
    val hwmD = CountTableLib.deserializeHotWindowManager(outFile)
    val fvd1 = hwmD.featurizePoint(getPoint1(), featurizeWithCounts,
      featurizeWithProbabilities, addBackingNoise)
    shouldAllBeNonZero(fvd1)
    val fvd2 = hwmD.featurizePoint(getPoint2(), featurizeWithCounts,
      featurizeWithProbabilities, addBackingNoise)
    shouldAllBeNonZero(fvd2)
    val fvd3 = hwmD.featurizePoint(getPoint3(), featurizeWithCounts,
      featurizeWithProbabilities, addBackingNoise)
    shouldAllBeNonZero(fvd3)
    val fvd4 = hwmD.featurizePoint(getPoint4(), featurizeWithCounts,
      featurizeWithProbabilities, addBackingNoise)
    shouldAllBeNonZero(fvd4)
  }
}