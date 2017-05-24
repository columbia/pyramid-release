package edu.columbia.cs.pyramid

import java.text.DecimalFormat

import scala.collection.mutable.ArrayBuffer
import scala.io.Source

object PyramidParser {

  // Regex for splitting features that may be separated by white space.
  val wsReg = "\\s+".r
  // Namespaces are delineated by a bar and an optional name.
  val barRegex = "\\|\\S*".r

  // Formatters to limit the number of digits.
  val formatters = Array(
    // new DecimalFormat("#.########"),
    // new DecimalFormat("##.#######"),
    // new DecimalFormat("###.######"),
    // new DecimalFormat("####.#####"),
    // new DecimalFormat("#####.####"),
    // new DecimalFormat("######.###"),
    // new DecimalFormat("#######.##"),
    // new DecimalFormat("########.#")

    // less decimals to test, cause it takes a lot of space
    new DecimalFormat("#.####"),
    new DecimalFormat("##.###"),
    new DecimalFormat("###.##"),
    new DecimalFormat("####.#")
  )

  /**
    * Converts a double to a scientific notation string.
    *
    * @param d
    * @return
    */
  def doubleToSN(d: Double): String = {
    val p = math.floor(math.log10(d)).toInt
    return "%1.3fE%d" format(d * math.pow(10, -p), p)
  }


  /**
    * Custom formatter that's hopefully faster.
    *
    * @param inputDouble
    * @return
    */
  def formatDouble(inputDouble: Double,
                   maxScale: Int = 10,
                   decimalDigits: Int = 4): String = {
    require(inputDouble >= 0)
    require(maxScale > 1)
    require(decimalDigits > 0)

    var double = inputDouble
    var scientificNotation = true
    val sb = new StringBuilder

    // Just return 0 if the input is zero.
    if (double == 0) {
      return "0"
    } else if (double >= 1) {
      val dInt = double.toLong
      sb.append(dInt)
      double = double - dInt
      scientificNotation = false
      if (double == 0.0) {
        return sb.toString()
      }
    }

    var scale = 0
    while (scale < maxScale && double < 1 && inputDouble < 1) {
      double *= 10
      scale += 1
    }

    if (scale == maxScale && double < 1) {
      if (double < 1) {
        return "0"
      } else {
        return sb.toString()
      }
    }
    val digitsToUse = if (scientificNotation) decimalDigits else 2
    if (inputDouble < 1) {
      sb.append(double.toLong)
    }
    sb.append(".")
    double = double - double.toLong

    var previousVal = -1l
    var previousValCount = 0
    var i = 0
    while ((i < digitsToUse && double > 0) || i == 0){
      double *= 10
      val dInt = double.toLong
      sb.append(double.toLong)
      double = double - dInt
      if (dInt == previousVal) {
        previousValCount += 1
      } else {
        previousVal = dInt
        previousValCount = 0
      }
      i += 1
    }

    if (previousValCount > 0) {
      sb.delete(sb.length - previousValCount, sb.length - 1)
    }

    if (scientificNotation) {
      sb.append("E-")
      sb.append(scale)
    }

    return sb.toString
  }

  /**
    * Converts a double to a string which may be in scientific notation.
    *
    * @param d
    * @return
    */
  def doubleToString(d: Double, decimalDigits: Int): String = {
    return formatDouble(d, decimalDigits = decimalDigits)
  }

  /**
    * Parses a line from the percentile file from the format:
    * int | (double )+
    *
    * @param line
    * @return
    */
  def parsePercentileLine(line: String): (Int, Array[Double]) = {
    // val splits = line.toString.split("\\|")
    val splits = barRegex.split(line.trim)
    require(splits.length == 2)
    val feature = splits(0).trim
    val percentiles = splits(1).trim.split(" ").map { x =>
      x.trim.toDouble
    }.toArray
    return (feature.trim.toInt, percentiles)
  }

  /**
    * Reads the percentile file and returns a map of FeatureIndex -> Percentiles
    *
    * @param percentileFile
    * @return
    */
  def readPercentileFile(percentileFile: String): Map[Int, Array[Double]] = {
    return Source.fromFile(percentileFile).getLines().map { line =>
      val (feature: Int, percentiles: Array[Double]) =
        parsePercentileLine(line)
      (feature, percentiles.sorted)
    }.toMap
  }

  /**
    * Parses a label string in the format
    * <label> [weight]
    *
    * @param labelString
    * @return Label and an optional weight.
    */
  def parseLabelString(labelString: String):
  (Double, Option[Double]) = {
    val splitLabel = wsReg.split(labelString)
    return splitLabel.length match {
      case 1 => (splitLabel(0).trim.toDouble, None)
      case 2 => (splitLabel(0).trim.toDouble, Some(splitLabel(1).trim.toDouble))
    }
  }

  /**
    * Parses a feature string and buckets values that are present in the
    * percentile file.
    *
    * @param featureString
    * @param percentiles
    * @param featureCount
    * @return An array of doubles as a feature vector.
    */
  def parseFeatureString(featureString: String,
                         percentiles: Map[Int, Array[Double]],
                         featureCount: Int): Array[Double] = {
    val featureVector = Array.fill[Double](featureCount)(0.0)
    wsReg.split(featureString.trim).zipWithIndex.foreach {
      case (f: String, index: Int) =>
        val splitF = f.split(":")
        splitF.length match {
          // There is no explicit key so use the index.
          case 1 => {
            val key = index
            featureVector(key) = bucketValue(key, splitF(0).trim, percentiles)
          }
          // There is an explicit key so use it.
          case 2 => {
            // val key = splitF(0).trim.toInt
            val key = index
            featureVector(key) = bucketValue(key, splitF(1).trim, percentiles)
          }
        }
    }
    return featureVector
  }

  /**
    * Parses a feature string and buckets values that are present in the
    * percentile file.
    *
    * @param featureString
    * @param percentiles
    * @param featureCount
    * @return An array of strings to be used as a feature vector.
    */
  def parseFeatureStringToString(featureString: String,
                                 percentiles: Map[Int, Array[Double]],
                                 featureCount: Int): Array[String] = {
    val featureVector = Array.fill[String](featureCount)("")
    wsReg.split(featureString.trim).zipWithIndex.foreach {
      case (f: String, index: Int) =>
        val splitF = f.split(":")
        splitF.length match {
          // There is no explicit key so use the index.
          case 1 => {
            val key = index
            featureVector(key) = bucketValueToString(key, splitF(0).trim, percentiles)
          }
          // There is an explicit key so use it.
          case 2 => {
            val key = splitF(0).trim.toInt
            featureVector(key) = bucketValueToString(key, splitF(1).trim, percentiles)
          }
        }
    }
    return featureVector
  }

  /**
    * Buckets values to percentiles if needed and returns it as a string.
    * Categorical values are not converted to hashcode.
    *
    * @param key
    * @param valueStr
    * @param percentiles
    * @return
    */
  def bucketValueToString(key: Int, valueStr: String,
                          percentiles: Map[Int, Array[Double]]): String = {
    try {
      val value = valueStr
      return percentiles.get(key) match {
        case Some(buckets) => bucketValue(value.toDouble, buckets).toString
        case None => value
      }
    } catch {
      case e: NumberFormatException => return valueStr
    }
  }

  /**
    * Buckets values to percentiles and returns the value as a string.
    * Categorical values are returned as a double version of their hash code.
    *
    * @param key
    * @param valueStr
    * @param percentiles
    * @return
    */
  def bucketValue(key: Int, valueStr: String,
                  percentiles: Map[Int, Array[Double]]): Double = {
    try {
      val value = valueStr.toDouble
      return percentiles.get(key) match {
        case Some(buckets) => bucketValue(value, buckets)
        case None => value
      }
    } catch {
      case e: NumberFormatException => return valueStr.hashCode.toDouble
    }
  }

  /**
    * Parses a file from a data file and returns the label, an optional weight,
    * and a feature vector. Categorical variables are converted by hashcode.
    *
    * @param line
    * @param percentiles
    * @param featureCount
    * @return
    */
  def parseDataFileLine(line: String, percentiles: Map[Int, Array[Double]],
                        featureCount: Int):
  (Double, Option[Double], Array[Double]) = {
    // val splits = line.trim.split("\\|")
    val splits = barRegex.split(line.trim)
    require(splits.length == 2)
    val labelString = splits(0).trim
    val featureString = splits(1).trim

    val (label: Double, weight: Option[Double]) = parseLabelString(labelString)
    val features = parseFeatureString(featureString, percentiles, featureCount)
    return (label, weight, features)
  }

  /**
    * Parses a file from a data file and returns part of the strings that
    * contain the features.
    *
    * @param line
    * @return
    */
  def parseFeaturesFromFileLine(line: String): String = {
    val splits = barRegex.split(line.trim)
    require(splits.length == 2)
    val featureString = splits(1).trim

    return featureString
  }

  /**
    * Parses a data file line and returns the label, an optional weight, and
    * a feature vector as a string. Categorical values are not converted by
    * hashcode.
    *
    * @param line
    * @param percentiles
    * @param featureCount
    * @return
    */
  def parseDataFileLineToString(line: String, percentiles: Map[Int, Array[Double]],
                                featureCount: Int):
  (Double, Option[Double], Array[String]) = {
    // val splits = line.trim.split("\\|")
    val splits = barRegex.split(line.trim)
    require(splits.length == 2)
    val labelString = splits(0).trim
    val featureString = splits(1).trim

    val (label: Double, weight: Option[Double]) = parseLabelString(labelString)
    val features = parseFeatureStringToString(featureString, percentiles, featureCount)
    return (label, weight, features)
  }

  /**
    * Rounds the value down to the nearest percentile.
    *
    * @param value
    * @param percentiles
    * @return
    */
  def bucketValue(value: Double, percentiles: Array[Double]): Double = {
    percentiles.foreach { v =>
      if (value <= v) {
        return v
      }
    }
    return percentiles(percentiles.length - 1)
  }

  /**
    * Converts the label and the feature vector to a line in vw form with the
    * features namespaced to f.
    *
    * @param label
    * @param featureVector
    * @return
    */
  def getOutLine(label: Double,
                 featureVector: Array[Double],
                 decimalDigits: Int): String = {
    var sb = new StringBuilder
    sb = sb
      .append(label.toString)
      .append(" |f ")
    val fvl = featureVector.length
    var index = 0
    for (i <- 0 until fvl) {
      sb.append("f")
      sb.append(index)
      sb.append(":")
      sb = sb.append(doubleToString(featureVector(i), decimalDigits))
      if (i != fvl - 1) {
        sb = sb.append(" ")
      }
      index += 1
    }
    return sb.append("\n").toString()
  }

  /**
    * Combines all of the feature vectors in non overlapping namespaces.
    *
    * @param label
    * @param featureVectors
    * @return
    */
  def getOutLine(label: Double,
                 featureVectors: ArrayBuffer[Array[Double]],
                 decimalDigits:Int): String = {
    var sb = new StringBuilder().append(label).append(" ")
    var currentOffset = 1
    featureVectors.foreach{ featureVector =>
      sb = sb.append("|").append(currentOffset).append(" ")
      featureVector.zipWithIndex.foreach{
        case(feature: Double, index: Int) =>
          val featureString = doubleToString(feature, decimalDigits)
          sb = sb.append(index).append(":").append(featureString).append(" ")
      }
      currentOffset += featureVector.length
    }
    return sb.append("\n").toString()
  }

  /**
    * Parses labels to an array of doubles.
    *
    * @param labels
    * @return
    */
  def parseLabels(labels: Seq[String]): Seq[Double] = {
    return labels.map(l => l.toDouble)
  }
}
