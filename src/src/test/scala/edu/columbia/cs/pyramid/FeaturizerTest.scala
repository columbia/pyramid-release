package edu.columbia.cs.pyramid

import org.scalatest.{FunSuite, Matchers}
import scala.util.Random

class FeaturizerTest extends FunSuite with Matchers {

  val featurizerName = "featurizer"
  val isBackingNoise = false
  val noiseLaplaceB: Option[Double] = None
  val cmsEpsilon = 0.00001
  val cmsDelta = 0.00001

  def instantiateLocalLabelCounter(name: String): LabelCounter = {
    return new LocalLabelCounter()
  }

  def instantiateCMS(name: String, laplaceB: Option[Double]): ApproximateCountTable = {
    return new ApproximateCountTable(cmsEpsilon, cmsDelta, isBackingNoise,
      laplaceB)
  }

  def getInstantiateCMS(seeds: Option[Array[Int]]):
  (String, Option[Double]) => CountTable = {
    def instantiateCMSSeeds(name: String, laplaceB: Option[Double]): ApproximateCountTable = {
      return new ApproximateCountTable(cmsEpsilon, cmsDelta, isBackingNoise,
        laplaceB, seeds = seeds)
    }
    return instantiateCMSSeeds
  }

  test("Featurization should fail if a feature is larger than the feature count.") {
    val labels: Seq[Double] = Seq(1, 2)
    val dependentFeatures = Seq(Seq(1, 2, 3), Seq(4, 5, 6))
    an[IllegalArgumentException] should be thrownBy new Featurizer(featurizerName, labels,
      5, dependentFeatures, instantiateCMS, instantiateLocalLabelCounter)
  }

  test("Get key should index into feature vector correctly") {
    val labels: Seq[Double] = Seq(1, 2)
    val dependentFeatures = Seq(Seq(1, 2, 3), Seq(4, 5, 6))
    val featurizer = new Featurizer(featurizerName, labels, 10, dependentFeatures,
      instantiateCMS, instantiateLocalLabelCounter)

    val keyFeatures1 = Array[Int](1)
    val featureVector1 = Array[Double](1.0, 2.0, 3.0)

    val keyFeatures2 = Array[Int](1, 2, 4)
    val featureVector2 = Array[Double](1.0, 2.0, 3.0, 4.0, 5.0)

    val key1: Array[Double] = featurizer.getKey(keyFeatures1, featureVector1,
      labels(0))
    val key2: Array[Double] = featurizer.getKey(keyFeatures2, featureVector2,
      labels(1))

    key1 should have length (keyFeatures1.length + 1)
    key1(0) should be equals featureVector1(2)

    key2 should have length (keyFeatures2.length + 1)
    key2(0) should be equals featureVector2(1)
    key2(1) should be equals featureVector2(2)
    key2(2) should be equals featureVector2(4)
  }

  // TODO - This is no longer true but I'm leaving it here for reference.
  test("getCountTableKeys should throw an exception if there is an empty array.") {
    /*
    val labels: Seq[Double] = Seq(1, 2)
    val dependentFeatures1 = Seq[Seq[Int]](Seq())
    val dependentFeatures2 = Seq[Seq[Int]](Seq(1, 2, 3), Seq(), Seq(1, 2, 3, 4))

    an[IllegalArgumentException] should be thrownBy new Featurizer(featurizerName, labels,
      5, dependentFeatures1, instantiateCMS, instantiateLocalLabelCounter)
    an[IllegalArgumentException] should be thrownBy new Featurizer(featurizerName, labels,
      5, dependentFeatures2, instantiateCMS, instantiateLocalLabelCounter)
      */
  }

  test("CountTable keys with no dependent features should have single element arrays.") {
    val labels: Seq[Double] = Seq(1, 2)
    val featureCount = 5
    val dependentFeatures = Seq[Seq[Int]]()
    val featurizer = new Featurizer(featurizerName, labels, 10, dependentFeatures,
      instantiateCMS, instantiateLocalLabelCounter)

    val ctKeys1: Seq[CountTableID] = featurizer.getCountTableKeys(featureCount,
      dependentFeatures)
    ctKeys1.length should equal(featureCount)
    ctKeys1 should have length (featureCount)
    ctKeys1.foreach { ctId: CountTableID =>
      ctId.featureVectorIndices should have length 1
    }
  }

  test("Featurizer should produce the correct feature vector with 2 labels and 1 feature") {
    val labels: Seq[Double] = Seq[Double](1.0, 2.0)
    val featureCount = 1
    val dependentFeatures = Seq[Seq[Int]]()
    val featurizer = new Featurizer(featurizerName, labels, featureCount, dependentFeatures,
      instantiateCMS, instantiateLocalLabelCounter)

    val featureVector1 = Array[Double](1.0)
    val fv1_1 = new LabeledPoint(1.0, featureVector1)
    val fv1_1_count = 100
    (0 until fv1_1_count).foreach { i =>
      featurizer.addObservation(fv1_1)
    }
    val fv1_2 = new LabeledPoint(2.0, featureVector1)
    val fv1_2_count = 15000
    (0 until fv1_2_count).foreach { i =>
      featurizer.addObservation(fv1_2)
    }
    val fv1Count = (fv1_1_count + fv1_2_count).toDouble

    val featureVector2 = Array[Double](2.0)
    val fv2_1 = new LabeledPoint(1.0, featureVector2)
    val fv2_1_count = 75
    (0 until fv2_1_count).foreach { i =>
      featurizer.addObservation(fv2_1)
    }
    val fv2_2 = new LabeledPoint(2.0, featureVector2)
    val fv2_2_count = 20000
    (0 until fv2_2_count).foreach { i =>
      featurizer.addObservation(fv2_2)
    }
    val fv2Count = (fv2_1_count + fv2_2_count).toDouble

    val fv1_1Featurized = featurizer.featurizePoint(fv1_1)
    val fv1_2Featurized = featurizer.featurizePoint(fv1_2)
    val fv2_1Featurized = featurizer.featurizePoint(fv2_1)
    val fv2_2Featurized = featurizer.featurizePoint(fv2_2)

    println(fv1_1Featurized.mkString(","))
    println(fv1_2Featurized.mkString(","))
    println(fv2_1Featurized.mkString(","))
    println(fv2_2Featurized.mkString(","))

    fv1_1Featurized should have length 4
    fv1_2Featurized should have length 4
    fv2_1Featurized should have length 4
    fv2_2Featurized should have length 4

    fv1_1Featurized should be equals fv1_2Featurized
    fv2_1Featurized should be equals fv2_2Featurized

    fv1_1Featurized should contain(fv1_1_count)
    fv1_1Featurized should contain(fv1_2_count)

    fv2_1Featurized should contain(fv2_1_count)
    fv2_1Featurized should contain(fv2_2_count)

    fv1_1Featurized should contain(fv1_1_count / fv1Count)
    fv1_1Featurized should contain(fv1_2_count / fv1Count)

    fv2_1Featurized should contain(fv2_1_count / fv2Count)
    fv2_1Featurized should contain(fv2_2_count / fv2Count)
  }

  test("Featurizer merging should produce the same output as unmerged") {
    val labels: Seq[Double] = Seq[Double](1.0, 2.0)
    val featureCount = 1
    val dependentFeatures = Seq[Seq[Int]]()


    val cmsDepth = math.ceil(math.log(1 / cmsDelta)).toInt
    val hashSeeds: Option[Array[Int]] = Some(Array.fill[Int](cmsDepth) {
      Random.nextInt()
    })
    val getCMSFunction = getInstantiateCMS(hashSeeds)

    val featurizer1 = new Featurizer(featurizerName, labels, featureCount,
      dependentFeatures, getCMSFunction, instantiateLocalLabelCounter)

    val featurizer2 = new Featurizer(featurizerName, labels, featureCount,
      dependentFeatures, getCMSFunction, instantiateLocalLabelCounter)

    // featurizer3 will be featurizer1 + featurizer 2
    val featurizer3 = new Featurizer(featurizerName, labels, featureCount,
      dependentFeatures, getCMSFunction, instantiateLocalLabelCounter)

    // featurizer 4 will contain all points of featurizer1 and featurizer2
    val featurizer4 = new Featurizer(featurizerName, labels, featureCount,
      dependentFeatures, getCMSFunction, instantiateLocalLabelCounter)


    val featureVector1 = Array[Double](1.0)
    val fv1_1 = new LabeledPoint(1.0, featureVector1)
    val fv1_1_count = 100
    (0 until fv1_1_count).foreach { i =>
      featurizer1.addObservation(fv1_1)
      featurizer4.addObservation(fv1_1)
    }
    (0 until fv1_1_count).foreach { i =>
      featurizer2.addObservation(fv1_1)
      featurizer4.addObservation(fv1_1)
    }
    val fv1_2 = new LabeledPoint(2.0, featureVector1)
    val fv1_2_count = 15000
    (0 until fv1_2_count).foreach { i =>
      featurizer1.addObservation(fv1_2)
      featurizer4.addObservation(fv1_2)
    }
    val fv1Count = (fv1_1_count + fv1_2_count).toDouble

    val featureVector2 = Array[Double](2.0)
    val fv2_1 = new LabeledPoint(1.0, featureVector2)
    val fv2_1_count = 75
    (0 until fv2_1_count).foreach { i =>
      featurizer2.addObservation(fv2_1)
      featurizer4.addObservation(fv2_1)
    }
    val fv2_2 = new LabeledPoint(2.0, featureVector2)
    val fv2_2_count = 20000
    (0 until fv2_2_count).foreach { i =>
      featurizer1.addObservation(fv2_2)
      featurizer2.addObservation(fv2_2)
      featurizer4.addObservation(fv2_2)
      featurizer4.addObservation(fv2_2)
    }

    featurizer3.merge(featurizer1)
    featurizer3.merge(featurizer2)

    val fv1_3Featurized = featurizer3.featurizePoint(fv1_1)
    val fv1_4Featurized = featurizer4.featurizePoint(fv1_1)
    println(fv1_3Featurized.mkString(","))
    println(fv1_4Featurized.mkString(","))
    fv1_3Featurized.zip(fv1_4Featurized).foreach {
      case (f1: Double, f2: Double) =>
        f1 should be(f2)
    }
    val fv2_3Featurized = featurizer3.featurizePoint(fv2_2)
    val fv2_4Featurized = featurizer4.featurizePoint(fv2_2)
    println(fv2_3Featurized.mkString(","))
    println(fv2_4Featurized.mkString(","))
    fv2_3Featurized.zip(fv2_4Featurized).foreach {
      case (f1: Double, f2: Double) =>
        f1 should be(f2)
    }
  }
}
