package edu.columbia.cs.pyramid

import scala.collection.mutable

/**
  * Implementation of a LabelCounter using a HashMap.
  *
  */
class LocalLabelCounter extends LabelCounter {
  var total: Double = 0.0
  val countMap: mutable.HashMap[Double, Double] =
    new mutable.HashMap[Double, Double]()

  override def increment(labelValue: Double, by: Double = 1): Unit = {
    total    += by
    countMap += (labelValue -> (countMap.getOrElse(labelValue, 0.0) + by))
  }

  override def labelProbability(labelValue: Double): Double = {
    if (total == 0) return 0.0
    countMap.get(labelValue) match {
      case Some(count) => count / total
      case None        => 0.0
    }
  }

  override def cleanup(): Unit = {
  }

  /**
    * Returns a sequence of all labels in the counter.
    *
    * @return
    */
  override def getLabels(): Seq[Double] = {
    return countMap.keySet.toSeq
  }

  /**
    * Returns the number of times the label has been observed.
    *
    * @param label
    * @return
    */
  override def getLabelCount(label: Double): Double = {
    return countMap.getOrElse(label, 0.0)
  }
}
