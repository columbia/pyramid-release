package edu.columbia.cs.pyramid


import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.{HashMap => MHashMap}
import scala.util.Random

case class LabeledPoint(label: Double, featureVector: Array[Double]) {
  override def toString(): String = {
    return label.toString + " | " + featureVector.mkString(" ")
  }
}

case class DefaultProbaCriteriaConfig(
  defaultProbaTotalCountCriterion: Boolean = true,
  defaultProbaVarianceCriterion: Boolean   = false,
  defaultProvaStdDevMultiplier: Double     = 1.0
)

/**
  * This case class is used to uniquely identify each count table. The
  * featureVectorIndices are used to index into the feature vector to create
  * keys that index into the count table.
  *
  * @param featureVectorIndices FeatureVector indices that are used to create
  *                             keys that will increment the counts.
  */
case class CountTableID(featureVectorIndices: Array[Int])
  extends Ordered[CountTableID] {

  // We just need some kind of stable ordering.
  override def compare(that: CountTableID): Int = {
    var i = 0
    val thisL = this.featureVectorIndices.length
    val thatL = that.featureVectorIndices.length
    if (thisL != thatL) {
      return thisL - thatL
    }
    while (i < thisL) {
      val thisI = this.featureVectorIndices(i)
      val thatI = that.featureVectorIndices(i)
      if (thisI != thatI) {
        return thisI - thatI
      }
      i += 1
    }
    return 0
  }

  override def toString(): String = {
    featureVectorIndices.mkString(",")
  }

  override def hashCode(): Int = {
    var hc: Int = 0
    for (v <- featureVectorIndices) {
      hc ^= v
    }
    return hc
  }

  override def equals(that: Any): Boolean = that match {
    case that: CountTableID => return this.compare(that) == 0
    case _ => false
  }
}

class Featurizer(val name: String,
                 val labels: Seq[Double],
                 val featureCount: Int,
                 val dependentFeatures: Seq[Seq[Int]] = Seq[Seq[Int]](),
                 val getCountTable: (String, Option[Double]) => CountTable,
                 val getLabelCounter: (String) => LabelCounter,
                 val tableSpecificCreation: Array[(String, Option[Double]) => CountTable] = null,
                 val tableSpecificNoise: Array[Double] = null,
                 val countTablePerLabel: Boolean = false)
  extends PointFeaturization {

  require(labels.length > 0)
  if (tableSpecificCreation != null) {
    require(tableSpecificCreation.length == featureCount + dependentFeatures.length)
  }
  if (tableSpecificNoise != null) {
    require(tableSpecificNoise.length == featureCount + dependentFeatures.length)
  }

  val countTables = instantiateCountTables()
  val labelCounter = instantiateLabelCounter()

  /**
    * Merges that featurizer into this featurizer.
    *
    * TODO - test that all other parameters are the same.
    *
    * @param that
    */
  def merge(that: Featurizer): Unit = {

    // TODO - The names aren't always going to be the same. If we have multiple hot windows in redis they won't have the same namespace.
    // require(that.name == name)
    require(that.featureCount == featureCount)
    require(countTables.length == that.countTables.length)
    require(countTables.map {case (tId, _) => tId }.toSet == that.countTables.map { case (tId, _) => tId }.toSet)

    (0 until countTables.length).foreach{ index =>
      val (thisID, thisCountTable) = countTables(index)
      val (thatID, thatCountTable) = that.countTables(index)
      require(thisID == thatID)
      thisCountTable.merge(thatCountTable)
    }
    labelCounter.merge(that.labelCounter)
  }

  /**
    * Creates a count based feature vector from the labeled point. The feature
    * vector is created ...
    *
    * @param point The labeled point to be featurized.
    * @return A double array to be used as a feature vector.
    */
  def featurizePoint(point: LabeledPoint,
                     removeFirstLabel: Boolean = false,
                     featurizeWithCounts: Boolean = true,
                     featurizeWithTotalCount: Boolean = false,
                     featurizeWithProbabilities: Boolean = true,
                     addBackingNoise: Boolean = false,
                     defaultProbaCriteria: DefaultProbaCriteriaConfig = DefaultProbaCriteriaConfig(),
                     window: Option[Int] = None):
  Array[Double] = {

    /**
      * Window is not defined for this class so we'd rather it crash and let
      * us know than just sail on.
      */
    require(window.isEmpty)

    val featureVector = new ArrayBuffer[Double]()
    val labelsToFeaturize = if (removeFirstLabel) labels.drop(1) else labels
    countTables.foreach { case (ctID: CountTableID, countTable: CountTable) =>
      val counts = MHashMap[Double, Double]()
      val fvTotal = labels.map { l =>
        val key = getKey(ctID.featureVectorIndices, point.featureVector, l)
        val count = countTable.pointQuery(key, addBackingNoise)
        counts(l) = count
        count
      }.sum
      if (featurizeWithTotalCount) {
        featureVector.append(fvTotal)
      }
      labelsToFeaturize.foreach { label =>
        // The total number of elements counted with this feature set.
        var featureCount: Double = counts(label)
        if (featurizeWithCounts) {
          featureVector.append(featureCount)
        }
        if (featurizeWithProbabilities) {
          val probability = getLabelProbability(label,
                                                labels.length,
                                                featureCount,
                                                fvTotal,
                                                countTable.noiseLaplaceB,
                                                defaultProbaCriteria,
                                                None)
          featureVector.append(probability)
        }
      }
    }
    return featureVector.toArray
  }

  def countForFeature(label: Double,
                      featureVector: Array[Double],
                      featureIndices: Array[Int],
                      addBackingNoise: Boolean = false,
                      window: Option[Int] = None):
  Double = {
    require(window.isEmpty)
    countTables.foreach {
      case (ctID: CountTableID, countTable: CountTable) =>
        if (ctID.featureVectorIndices.sameElements(featureIndices)) {
          val key = getKey(ctID.featureVectorIndices, featureVector, label)
          val count = countTable.pointQuery(key, addBackingNoise)
          return count
        }
    }
    return 0.0
  }

  /**
    * Returns the probability of the label or the default label if the
    * featureCount of fvTotal are close to zero and dominated by noise.
    *
    * @param label        The label to calculate probability
    * @param featureCount The number of times the label has appeared with the
    *                     feature.
    * @param fvTotal      The number of times this feature has appeared in total
    *                     with any label.
    * @return P(L|F) or P(L) if F has been observed a small number of times.
    */
  def getLabelProbability(label: Double,
                          labelCounts: Int,
                          featureCount: Double,
                          fvTotal: Double,
                          window: Option[Int]): Double = {
    // Again, windows aren't defined for this class.
    require(window.isEmpty)
    return getLabelProbability(label, labelCounts, featureCount, fvTotal, None,
      DefaultProbaCriteriaConfig(), None)
  }

  def getLabelProbability(label: Double,
                          labelCounts: Int,
                          featureCount: Double,
                          fvTotal: Double,
                          noiseB: Option[Double],
                          defaultProbaCriteria: DefaultProbaCriteriaConfig,
                          window: Option[Int]): Double = {

    require(window.isEmpty)

    // should probably be 1 or 2
    val stdDevInterval = defaultProbaCriteria.defaultProvaStdDevMultiplier
    val defaultProba = labelCounter.labelProbability(label)
    val countProba   = featureCount / fvTotal
    if (fvTotal == 0) { return defaultProba }

    if (defaultProbaCriteria.defaultProbaTotalCountCriterion) {
      val lowerLimit = noiseB match {
        case Some(b) => stdDevInterval * math.sqrt(2 * math.pow(b, 2) * labelCounts)
        case None    => 0.0
      }
      if (fvTotal < lowerLimit) { return defaultProba }
    }

    if (defaultProbaCriteria.defaultProbaVarianceCriterion) {
      val b = noiseB match {
        case Some(b) => b
        case None => 0.0
      }
      val stdDev       = probaStdDev(fvTotal, countProba, b)
      if (countProba - stdDevInterval * stdDev <= defaultProba &&
        defaultProba <= countProba + stdDevInterval * stdDev) {
        // the default proba is within stdDevInterval std-dev from the estimated probability,
        // so we consider the estimate is not significant.
        return defaultProba
      } else {
        // the default proba is not within the confidence interval of the
        // countProba, so we can use the count proba.
        return countProba
      }
    } else {
      return countProba
    }
  }

  def probaStdDev(totCount: Double, proba: Double, noiseB: Double): Double = {
    val noiseVar = if (noiseB > 0) {
      2 * math.pow(noiseB, 2)
    } else {
      0
    }
    val bernouilliVar = proba * (1 - proba)
    val totVar = (totCount * bernouilliVar + noiseVar) / math.pow(totCount, 2)
    math.sqrt(totVar)
  }

  /**
    * Featurizes all points in the sequence.
    *
    * @param points Points to featurize.
    * @return Sequence of featurized points.
    */
  def featurizePoints(points: Seq[LabeledPoint],
                      window: Option[Int] = None): Seq[Array[Double]] = {
    // Again, not defined.
    require(window.isEmpty)
    return points.map { point =>
      featurizePoint(point)
    }
  }

  /**
    * Adds the observed point and updates the internal count tables.
    *
    * @param point The point to update the internal count tables.
    */
  def addObservation(point: LabeledPoint, by: Double = 1.0): Unit = {
    labelCounter.increment(point.label, 1.0)
    countTables.foreach {
      case (ctID: CountTableID, countTable: CountTable) =>
        val key = getKey(ctID.featureVectorIndices, point.featureVector,
          point.label)
        countTable.increment(key, 1.0)
    }
  }

  /**
    * Adds all points in the sequence to the featurizer.
    *
    * @param points
    */
  def addObservations(points: Seq[LabeledPoint]): Unit = {
    points.foreach { point =>
      addObservation(point)
    }
  }

  /**
    * Creates a Key to index into the count table using the featureVector
    * and label.
    *
    * @param keyFeatures   The indexes of features used to create the key.
    * @param featureVector The features from which the key will be created.
    * @return
    */
  def getKey(keyFeatures: Array[Int], featureVector: Array[Double],
             label: Double): Array[Double] = {
    val returnArray = new Array[Double](keyFeatures.length + 1)
    keyFeatures.zipWithIndex.foreach {
      case (featureVectorIndex: Int, returnArrayIndex: Int) =>
        returnArray(returnArrayIndex) = featureVector(featureVectorIndex)
    }
    returnArray(keyFeatures.length) = label
    return returnArray
  }

  /**
    * Returns a sequence of Int Arrays that will be used as the keys to access
    * count tables. It will also ensure that each feature is only in one key and
    * will throw an exception if this is violated.
    *
    * @param featureCount      The number of features to be used in the model.
    * @param dependentFeatures Sequence of sequences where each subsequence
    *                          contains features that should be counted
    *                          together.
    * @return Seq of arrays used to access count tables.
    */
  def getCountTableKeys(featureCount: Int,
                        dependentFeatures: Seq[Seq[Int]]):
  Seq[CountTableID] = {
    if (!dependentFeatures.flatten.isEmpty &&
      dependentFeatures.flatten.max >= featureCount) {
      throw new IllegalArgumentException("Cannot have a dependent feature " +
        "that is larger than the number of features.")
    }

    val independentFeatures = (0 until featureCount).map { f => Seq(f) }

    val featureKeys = (independentFeatures ++ dependentFeatures).distinct

    val retSeq = featureKeys.map { featureKey =>
      val ctKey = new CountTableID(featureKey.toArray)
      ctKey
    }

    return retSeq
  }

  /**
    * Instantiates a count table for set of dependent features and for each
    * independent feature and label. Ensures that each feature only appears in
    * one dependent compound key.
    *
    * @return A map of feature sets to tables used for counting those features.
    */
  private def instantiateCountTables():
  Array[(CountTableID, CountTable)] = {
    // Ensure that the keys are sorted.
    val countTableKeys = getCountTableKeys(featureCount, dependentFeatures).sorted
    if (tableSpecificNoise != null) println(tableSpecificNoise.mkString(" | "))
    return countTableKeys.zipWithIndex.toArray.map { case (key, i) =>
      val noiseLaplaceB: Option[Double] = if (tableSpecificNoise != null) Some(tableSpecificNoise(i)) else None
      val tableName = name + key.featureVectorIndices.mkString(",")
      val tableInstantiation = if (tableSpecificCreation != null) tableSpecificCreation(i) else getCountTable
      // println("call creation for table: " + i + " key: " + key.featureVectorIndices.mkString(","))
      (key, tableInstantiation(tableName, noiseLaplaceB))
    }
  }

  private def instantiateLabelCounter(): LabelCounter = {
    getLabelCounter(name)
  }

  def cleanup() = {
    countTables.foreach { case (_, ct) => ct.cleanup() }
 }
}
