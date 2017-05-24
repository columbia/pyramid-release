package edu.columbia.cs.pyramid

import scala.StringBuilder
import scala.collection.mutable

/**
  * A featurizer implementation that will keep a featurizer for the entire time
  * period and then also for disjoint windows.
  *
  */
class WindowedFeaturizer(val name: String,
                         val windowSize: Long,
                         val labels: Seq[Double],
                         val featureCount: Int,
                         val dependentFeatures: Seq[Seq[Int]] = Seq[Seq[Int]](),
                         val getCountTable: (String, Option[Double]) => CountTable,
                         val getLabelCounter: (String) => LabelCounter,
                         val tableSpecificCreation: Array[(String, Option[Double]) => CountTable] = null,
                         val tableSpecificNoise: Array[Double] = null,
                         val countTablePerLabel: Boolean = false
                        ) extends PointFeaturization {

  // If the window size is zero then it does not make sense.
  require(windowSize > 0)

  // The total number of observations seen. Used to roll the window.
  private var totalObservations: Int = 0

  // The current window.
  private var currentWindow: Int = 0;

  // Featurizer used for the entire observation set.
  private val fullFeaturizer = new Featurizer(name, labels, featureCount,
    dependentFeatures, getCountTable, getLabelCounter, tableSpecificCreation,
    tableSpecificNoise, countTablePerLabel)

  private val windowedFeaturizers = new mutable.HashMap[Int, Featurizer]()

  private def getFeaturizer(window: Int): Featurizer = {
    return windowedFeaturizers.get(window) match {
      case Some(f) => f
      case None => {
        val featurizerName = new StringBuilder("Window_").append(window)
          .append("_").append(name).toString
        val retFeaturizer = new Featurizer(featurizerName, labels, featureCount,
          dependentFeatures, getCountTable, getLabelCounter,
          tableSpecificCreation, tableSpecificNoise, countTablePerLabel)
        windowedFeaturizers.put(window, retFeaturizer)
        retFeaturizer
      }
    }
  }

  override def featurizePoint(point: LabeledPoint,
                              removeFirstLabel: Boolean,
                              featurizeWithCounts: Boolean,
                              featurizeWithTotalCount: Boolean,
                              featurizeWithProbabilities: Boolean,
                              addBackingNoise: Boolean,
                              defaultProbaCriteria: DefaultProbaCriteriaConfig = DefaultProbaCriteriaConfig(),
                              window: Option[Int]): Array[Double] = window match {
    case None => return fullFeaturizer.featurizePoint(point, removeFirstLabel,
      featurizeWithCounts, featurizeWithTotalCount, featurizeWithProbabilities,
      addBackingNoise, defaultProbaCriteria)
    case Some(w) => getFeaturizer(w).featurizePoint(point, removeFirstLabel,
      featurizeWithCounts, featurizeWithTotalCount, featurizeWithProbabilities,
      addBackingNoise, defaultProbaCriteria)
  }

  override def getLabelProbability(label: Double,
                                   labelCounts: Int,
                                   featureCount: Double,
                                   fvTotal: Double,
                                   window: Option[Int]): Double = window match {
    case None => fullFeaturizer.getLabelProbability(label, labelCounts,
      featureCount, fvTotal, window)
    case Some(w) => getFeaturizer(w).getLabelProbability(label, labelCounts,
      featureCount, fvTotal, window)
  }

  override def cleanup(): Unit = {
    fullFeaturizer.cleanup()
    windowedFeaturizers.values.map(_.cleanup())
  }

  override def addObservation(point: LabeledPoint, by: Double): Unit = {
    fullFeaturizer.addObservation(point, by)
    getFeaturizer(currentWindow).addObservation(point, by)
    totalObservations += 1
    if (totalObservations % windowSize == 0) {
      currentWindow += 1
    }
  }

  override def countForFeature(label: Double,
                               featureVector: Array[Double],
                               featureIndices: Array[Int],
                               addBackingNoise: Boolean,
                               window: Option[Int]): Double = window match {
    case None => fullFeaturizer.countForFeature(label, featureVector,
      featureIndices, addBackingNoise, None)
    case Some(w) => getFeaturizer(w).countForFeature(label, featureVector,
      featureIndices, addBackingNoise, None)
  }

  override def featurizePoints(points: Seq[LabeledPoint],
                               window: Option[Int]): Seq[Array[Double]] = window match {
    case None => fullFeaturizer.featurizePoints(points, None)
    case Some(w) => getFeaturizer(w).featurizePoints(points, None)
  }
}

