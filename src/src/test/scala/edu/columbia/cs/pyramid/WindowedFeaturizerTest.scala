package edu.columbia.cs.pyramid

import org.scalatest.{FunSuite, Matchers}

class WindowedFeaturizerTest extends FunSuite with Matchers {

  val windowSize = 100
  val labels = Array[Double](-1, 1)
  val dependentFeatures = Seq[Seq[Int]]()
  val featureCount = 4

  val isBackingNoise = false
  val noiseLaplaceB: Option[Double] = None
  val cmsEpsilon = 0.001
  val cmsDelta = 0.00001

  val label1 = -1.0
  val label2 = 1.0
  val testPoint1 = new LabeledPoint(label1, Array[Double](1, 2, 3, 4))
  val testPoint2 = new LabeledPoint(label2, Array[Double](1, 2, 3, 4))

  val epsilon = 0.00001

  def instantiateCMS(name: String, laplaceB: Option[Double]): ApproximateCountTable = {
    return new ApproximateCountTable(cmsEpsilon, cmsDelta, isBackingNoise,
      laplaceB)
  }

  def instantiateLocalLabelCounter(name: String): LabelCounter = {
    return new LocalLabelCounter()
  }

  private def getWindowedFeaturizer(): WindowedFeaturizer = {
    return new WindowedFeaturizer("windowed_featurizer", windowSize, labels,
      featureCount, dependentFeatures, instantiateCMS,
      instantiateLocalLabelCounter)
  }

  private def getFeaturizer(): Featurizer = {
    return new Featurizer("windowed_featurizer", labels, featureCount,
      dependentFeatures, instantiateCMS, instantiateLocalLabelCounter)
  }

  test("featurizer and windowed featurizer should return eqivalent results") {
    val wf = getWindowedFeaturizer()
    val f = getFeaturizer()
    (0 until (windowSize * + (windowSize / 2))).foreach { i =>
      wf.addObservation(testPoint1)
      f.addObservation(testPoint1)
    }
    (0 until (windowSize / 3)).foreach{ i=>
      wf.addObservation(testPoint2)
      f.addObservation(testPoint2)
    }
    val wfVector = wf.featurizePoint(testPoint1)
    val fVector = f.featurizePoint(testPoint1)
    wfVector.zip(fVector).map{
      case (f1: Double, f2: Double) =>
        f1 should equal(f2 +- epsilon)
    }

  }

  test("WindowedFeaturizer should featurize differently for most recent window.") {
    val wf = getWindowedFeaturizer()
    (0 until (windowSize * 8)).foreach{ i =>
      wf.addObservation(testPoint1)
      wf.addObservation(testPoint1)
      wf.addObservation(testPoint2)
    }
    (0 until (windowSize / 25)).foreach{ i =>
      wf.addObservation(testPoint1)
      wf.addObservation(testPoint2)
    }
    val fullFV = wf.featurizePoint(testPoint1)
    val windowFV = wf.featurizePoint(testPoint1, window=Some(8))
    windowFV.foreach{ f =>
      f should be > 0.0
    }
    fullFV.foreach{ f =>
      f should be > 0.0
    }
    fullFV.zip(windowFV).foreach {
      case (f1: Double, f2: Double) =>
        f1 should not equal(f2)
    }
  }

}
