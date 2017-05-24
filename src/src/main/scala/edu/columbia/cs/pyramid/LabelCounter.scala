package edu.columbia.cs.pyramid

/**
  * Generic counter trait for counting labels in Pyramid.
  *
  */
abstract class LabelCounter extends Serializable {
  /**
    * Increments the count for a label value.
    *
    * @param labelValue
    */
  def increment(labelValue: Double, by: Double = 1): Unit

  /**
    * Queries the counters to compute the probability of labelValue.
    *
    * @param labelValue
    * @return
    */
  def labelProbability(labelValue: Double): Double

  /**
    * Perform any needed cleanup.
    */
  def cleanup(): Unit

  /**
    * Merges that label counter into this label counter.
    *
    * @param that
    */
  def merge(that: LabelCounter): Unit = {
    val labels = that.getLabels().toSet.union(getLabels.toSet)
    labels.foreach{ l =>
      increment(l, that.getLabelCount(l))
    }
  }

  /**
    * Returns a sequence of all labels in the counter.
    *
    * @return
    */
  def getLabels(): Seq[Double]

  /**
    * Returns the number of times the label has been observed.
    *
    * @param label
    * @return
    */
  def getLabelCount(label: Double): Double

}
