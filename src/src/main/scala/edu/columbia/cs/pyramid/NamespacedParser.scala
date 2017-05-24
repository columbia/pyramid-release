package edu.columbia.cs.pyramid

import scala.collection.mutable

object NamespacedParser {

  // Regex for matching one of more whitespaces at the beginning of a line.
  private val firstWsReg = "^\\s+".r
  private val wsReg = "\\s+".r
  val defaultNamespace = ""

  /**
    * Parses a string split on '|' from a vw line and returns the namespace
    *
    * @param barSplit
    * @return
    */
  def parseNamespace(barSplit: String): (String, Seq[String]) = {
    firstWsReg.findFirstIn(barSplit) match {
      case Some(x) => {
        val wsSplit = wsReg.split(barSplit.trim)
        return (defaultNamespace, wsSplit)
      }
      case _ => {
        val wsSplit = wsReg.split(barSplit.trim)
        return (wsSplit.head, wsSplit.tail)
      }
    }
  }

  /**
    * Splits a string by | and returns a map of namespace -> string
    * representation.
    *
    * @param line The line to be parsed.
    * @return
    */
  def parseNamespaceLine(line: String): (String, mutable.HashMap[String, Seq[String]]) = {
    val splits = line.split('|')

    val labelString = splits.head
    val returnMap = mutable.HashMap[String, Seq[String]]()
    for (nsSplit <- splits.tail) {
      val (namespaceKey: String, namespaceFeatures: Seq[String]) = parseNamespace(nsSplit)
      val newFeatures: Seq[String] = returnMap.get(namespaceKey) match {
        case Some(features) => features ++ namespaceFeatures
        case _ => namespaceFeatures
      }
      returnMap.put(namespaceKey, newFeatures)
    }

    return (labelString, returnMap)
  }

  def toDoubleWithHashcode(str: String): Double = {
    try {
      return str.toDouble
    } catch {
      case _: Throwable => str.hashCode.toDouble
    }
  }

  def parseFeatureString(featureStrings: Seq[String],
                         percentiles: Map[Int, Array[Double]]):
  IndexedSeq[(String, Double)] = {
    featureStrings.zipWithIndex.map {
      case (featureString: String, index: Int) =>
        val splitFeature = featureString.split(':')
        splitFeature.length match {
          case 1 => {
            val key = index
            val featureValue: Double =
              PyramidParser.bucketValue(key, splitFeature(0).trim, percentiles)
            (key.toString, featureValue)
          }
          case 2 => {
            // TODO - Make percentiles work with Key values.
            val key = splitFeature(0)
            val featureValue: Double = toDoubleWithHashcode(splitFeature(1))
            (key, featureValue)
          }
        }
    }.toIndexedSeq
  }

  /**
    * Parses a data line into a map of namespace -> featureKey -> value.
    *
    * @param line Data line to be parsed.
    * @return Returns a label and namespace map.
    */
  def parseDataLine(line: String, percentiles: Map[Int, Array[Double]]):
  (Double, Map[String, IndexedSeq[(String, Double)]]) = {
    val (labelString: String, namespaces: mutable.HashMap[String, Seq[String]]) =
      parseNamespaceLine(line)
    val (label: Double, weight: Option[Double]) =
      PyramidParser.parseLabelString(labelString)
    val features = namespaces.map{
      case(namespaceKey: String, featureStrings: Seq[String]) =>
        val features: IndexedSeq[(String, Double)] =
          parseFeatureString(featureStrings, percentiles)
        (namespaceKey, features)

    }.toMap
    return (label, features)
  }

}
