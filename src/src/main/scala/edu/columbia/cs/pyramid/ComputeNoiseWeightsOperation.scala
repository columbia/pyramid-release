package edu.columbia.cs.pyramid

import java.util.concurrent.LinkedBlockingQueue

import com.fasterxml.jackson.databind.JsonNode

import scala.collection.mutable.ArrayBuffer

class ComputeNoiseWeightsOperation extends PyramidOperation {

  override val operationName: String = "computeNoiseWeights"

  override val pyramidConfigs = new PyramidConfigurations()
    // File containing quantiles into which continuous values will be bucketed.
    .addStringConfiguration("percentilesFile", true)
    // The percentile to be used to compute the noise weights.
    .addDoubleConfiguration("percentile", true)
    // File containing the serialized count table.
    .addStringConfiguration("countTableFile", true)
    // Input dataset to be used to compute the count weights.
    .addStringConfiguration("transformDataFilePath", true)
    // Path to which the results will be written.
    .addStringConfiguration("outDataFile", true)
    .addIntConfiguration("windowSize", false)
    // The number of features per record.
    .addIntConfiguration("featureCount", true)
    .addBoolConfiguration("multiGroups", false, Some(false))
    // Epsilon value to be used in differentially private weighting.
    .addDoubleConfiguration("epsilonDP", true)

  /**
    * Execute the associated operation with the configuration.
    */
  override def execute(jsonNode: JsonNode): Unit = {
    val parsedPyramidConfigs = pyramidConfigs.parseConfigurations(jsonNode)
    val epsilonDP = parsedPyramidConfigs.getOptionDoubleConfiguration("epsilonDP")
    val ws: Option[Int] = parsedPyramidConfigs.getOptionIntConfiguration("windowSize")
    val featurizer = ws match {
      case Some(ws) =>
        BuildCountTable.deserializeFeaturizer(parsedPyramidConfigs.getStringConfiguration("countTableFile"))
      case None =>
        CountTableLib.deserializeWindowedFeaturizer(parsedPyramidConfigs.getStringConfiguration("countTableFile"))
    }
    computeNoiseWeights(
      featurizer,
      parsedPyramidConfigs.getStringConfiguration("percentilesFile"),
      parsedPyramidConfigs.getStringConfiguration("transformDataFilePath"),
      parsedPyramidConfigs.getStringConfiguration("outDataFile"),
      parsedPyramidConfigs.getIntConfiguration("featureCount"),
      parsedPyramidConfigs.getDoubleConfiguration("percentile"),
      if (parsedPyramidConfigs.getBoolConfiguration("multiGroups")) None else Some(1),
      epsilonDP
    )
  }

  def computeNoiseWeights(featurizer: PointFeaturization,
                          percentileFile: String,
                          dataFile: String,
                          outFile: String,
                          featureCount: Int,
                          percentile: Double,
                          numberOfGroups: Option[Int],
                          epsilonDP: Option[Double] = Some(0.5),
                          countUpperBound: Int = 1000000,
                          bufferSize: Int = 50000) = {
    val percentiles = PyramidParser.readPercentileFile(percentileFile)

    val inputStream = CountTableLib.getInputStream(dataFile)
    val outWriter = CountTableLib.getOutputStream(outFile)

    val writeQueue = new LinkedBlockingQueue[String](bufferSize)
    val writeConsumer = new WriteConsumer(outWriter, writeQueue, bufferSize)
    val writeThread = new Thread(writeConsumer)
    writeThread.start()

    val vwIterator = new VWIterator(inputStream, bufferSize, bufferSize,
      percentiles, featureCount)

    var index = 0

    val parsedLinesPoints: Seq[(Option[Double], ArrayBuffer[Array[Double]])] =
    vwIterator.zipWithIndex.toSeq.par
              .map { case ((label: Double, weight: Option[Double], features: Array[Double], featureString: String), lineIndex: Int) =>
        val rawPoint = new LabeledPoint(label, features)

        val fullFeatureVectors = CountTableLib.getFeatureVectors(featurizer,
          rawPoint,
          true,   // remove first label
          false,  // featurizeWithCounts
          true,   // featurizeWithTotalCount
          false,  // featurizeWithProbabilites
          false,
          DefaultProbaCriteriaConfig(false, false, 0),
          None
        )

        (weight, fullFeatureVectors)
    }.toList

    val n         = parsedLinesPoints.length
    val _numberOfGroups = numberOfGroups match {
      case Some(g) => g
      case None    => math.pow(math.log(n)/math.log(2), 2).toInt + 1
    }
    val groupSize = (n / _numberOfGroups).toInt
    val pointCounts = Array.fill[Int](n) { 0 }
    val maxPointCount = math.sqrt(groupSize)
    var valid = false

    println("groups: ")
    println(_numberOfGroups)
    println("feature number: ")
    println(featureCount)
    val groupMedians = Array.fill[Array[ArrayBuffer[Double]]](_numberOfGroups){
      Array.fill[ArrayBuffer[Double]](featureCount){ArrayBuffer[Double]()}
    }
    // val medians = Array.fill[ArrayBuffer[Double]](featureCount){ArrayBuffer[Double]()}

    while(!valid) {
      var g = 0
      valid = true
      while(valid && g < _numberOfGroups) {
        util.Random.shuffle((0 until n).toList).take(groupSize).foreach { selectedIndex =>
          pointCounts(selectedIndex) += 1
          parsedLinesPoints(selectedIndex) match {
            case (weight: Option[Double], featureVectors: ArrayBuffer[Array[Double]]) =>
              val totCounts = featureVectors(0)
              totCounts.zipWithIndex.foreach { case (count, i) =>
                // println("group")
                // println(_numberOfGroups)
                // println(g)
                // println("i")
                // println(i)
                // println("")
                groupMedians(g)(i) += count
              }
          }
          if(pointCounts(selectedIndex) > maxPointCount) { valid = false }
        }
        g += 1
      }
    }

    writeConsumer.end()
    writeThread.join()

    groupMedians.foreach { (medians) =>
      medians.foreach { (allCounts) =>
        val percentileValue = epsilonDP match {
          case Some(epsilon) =>
            val featureEpsilon = epsilon / featureCount
            val beta           = featureEpsilon / 6.0
            val wrappedCounts: Array[Array[Double]] = allCounts.map { i =>
                    Array(math.max(1, math.min(i, countUpperBound)).toDouble)
            }.toArray
            val (q, sensitivity) = SmoothSensitivity.quantileFastSmoothSensitivity(
              wrappedCounts,
              percentile,
              countUpperBound,
              1,
              beta
            )
            val r = SmoothSensitivity.addNoise(q, sensitivity, featureEpsilon, None)(0)
            println("New feature:")
            println(epsilon)
            println(featureEpsilon)
            println(beta)
            println(wrappedCounts.length)
            println(q(0))
            println(sensitivity)
            println(r)
            println("")
            math.max(1, math.min(r, countUpperBound)).toInt

          case None =>
            val percentileIndex = (percentile * index).toInt
            val orderedCounts   = allCounts.sorted
            orderedCounts(percentileIndex)
        }

        outWriter.write(percentileValue.toInt.toString + " ")
      }
      outWriter.write("\n")
    }
    outWriter.close()
  }
}