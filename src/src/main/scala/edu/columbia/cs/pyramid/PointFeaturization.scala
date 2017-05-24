package edu.columbia.cs.pyramid

abstract class PointFeaturization extends Serializable {

  def featurizePoint(point: LabeledPoint,
                     removeFirstLabel: Boolean = false,
                     featurizeWithCounts: Boolean = true,
                     featurizeWithTotalCount: Boolean = false,
                     featurizeWithProbabilities: Boolean = true,
                     addBackingNoise: Boolean = false,
                     defaultProbaCriteria: DefaultProbaCriteriaConfig = DefaultProbaCriteriaConfig(),
                     window: Option[Int] = None): Array[Double]

  def countForFeature(label: Double,
                      featureVector: Array[Double],
                      featureIndices: Array[Int],
                      addBackingNoise: Boolean = false,
                      window: Option[Int] = None): Double

  def getLabelProbability(label: Double,
                          labelCounts: Int,
                          featureCount: Double,
                          fvTotal: Double,
                          window: Option[Int]): Double

  def featurizePoints(points: Seq[LabeledPoint],
                      window: Option[Int] = None): Seq[Array[Double]]

  def addObservation(point: LabeledPoint, by: Double = 1.0): Unit

  def cleanup()
}
