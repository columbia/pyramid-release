package edu.columbia.cs.pyramid

import java.util.concurrent.LinkedBlockingQueue

import com.fasterxml.jackson.databind.JsonNode

import scala.collection.mutable.ArrayBuffer


class TransformOperation extends PyramidOperation {

  override val pyramidConfigs = new PyramidConfigurations()
    // The number of features.
    .addIntConfiguration("featureCount", true)
    // Whether the noise should be held in a backing table rather than the main table.
    .addBoolConfiguration("backingTableNoise", false, Some(false))
    // Boolean noting if the first label should be included in the featurization.
    .addBoolConfiguration("removeFirstLabel", false, Some(false))
    // Boolean determining if the original features should be included in the featurization.
    .addBoolConfiguration("keepOriginalFeatures", false, Some(false))
    // Array of integers noting which of the original features should be kept.
    .addIntSeqConfiguration("originalFeaturesToKeep", false, Some(Seq[Int]()))
    // Boolean if the count for the specific feature should be included.
    .addBoolConfiguration("featurizeWithCounts", false, Some(false))
    // Boolean noting if the total count for all labels should be included
    .addBoolConfiguration("featurizeWithTotalCounts", false, Some(false))
    // Booolean noiting if the conditional probability should be included in the featurization.
    .addBoolConfiguration("featurizeWithProbabilities", false, Some(true))
    // Boolean noting if the count table should be updated with each of the.
    .addStringConfiguration("updateCountFile", false, None)
    // Featurize using the label probability if the total count is insufficiently large
    .addBoolConfiguration("useDefaultProbaTotalCountCriterion", false, Some(true))
    .addBoolConfiguration("useDefaultProbaVarianceCriterion", false, Some(false))
    .addDoubleConfiguration("defaultProbaStdDevMultiplier", false, Some(1.0))
    .addIntConfiguration("windowSize", false, None)
    .addIntConfiguration("decimalDigits", false, None)
    .addStringConfiguration("percentilesFile", true)
    .addStringConfiguration("countTableFile", true)
    .addStringConfiguration("transformDataFilePath", true)
    .addStringConfiguration("outDataFile", true)
    .addBoolConfiguration("addBackingTableNoise", false, Some(false))


  /**
    * Execute the associated operation with the configuration.
    */
  override def execute(jsonNode: JsonNode): Unit = {
    val parsedPyramidConfigs = pyramidConfigs.parseConfigurations(jsonNode)
    val windowSize: Option[Int] =
      parsedPyramidConfigs.getOptionIntConfiguration("windowSize")
    println("Count table file: " + parsedPyramidConfigs.getStringConfiguration("countTableFile"))
    val featurizer = BuildCountTable.deserializeFeaturizer(
      parsedPyramidConfigs.getStringConfiguration("countTableFile"))

    val updateCountFile: Option[String] = parsedPyramidConfigs.getOptionStringConfiguration("updatedCountFile")
    val decimalDigits = parsedPyramidConfigs.getOptionIntConfiguration("decimalDigits")

    val defaultProbabCriteria = DefaultProbaCriteriaConfig(
      parsedPyramidConfigs.getBoolConfiguration("useDefaultProbaTotalCountCriterion"),
      parsedPyramidConfigs.getBoolConfiguration("useDefaultProbaVarianceCriterion"),
      parsedPyramidConfigs.getDoubleConfiguration("defaultProbaStdDevMultiplier")
    )

    transformDataFile(featurizer,
      parsedPyramidConfigs.getStringConfiguration("percentilesFile"),
      parsedPyramidConfigs.getStringConfiguration("transformDataFilePath"),
      parsedPyramidConfigs.getStringConfiguration("outDataFile"),
      parsedPyramidConfigs.getIntConfiguration("featureCount"),
      parsedPyramidConfigs.getBoolConfiguration("removeFirstLabel"),
      parsedPyramidConfigs.getBoolConfiguration("keepOriginalFeatures"),
      parsedPyramidConfigs.getIntSeqConfiguration("originalFeaturesToKeep"),
      parsedPyramidConfigs.getBoolConfiguration("featurizeWithCounts"),
      parsedPyramidConfigs.getBoolConfiguration("featurizeWithTotalCounts"),
      parsedPyramidConfigs.getBoolConfiguration("featurizeWithProbabilities"),
      updatedCountFile = updateCountFile,
      addBackingTableNoise = parsedPyramidConfigs.getBoolConfiguration("addBackingTableNoise"),
      defaultProbaCriteria = defaultProbabCriteria,
      windowSize = None,
      decimalDigits = decimalDigits
    )
  }

  override val operationName: String = "transform"

  private def transformDataFile(featurizer: PointFeaturization,
                        percentileFile: String,
                        dataFile: String,
                        outFile: String,
                        featureCount: Int,
                        removeFirstLabel: Boolean = false,
                        keepOriginalFeatures: Boolean,
                        originalFeaturesToKeep: Seq[Int],
                        featurizeWithCounts: Boolean = false,
                        featurizeWithTotalCount: Boolean = false,
                        featurizeWithProbabilites: Boolean = true,
                        bufferSize: Int = 50000,
                        updatedCountFile: Option[String] = None,
                        updateCountTable: Boolean = false,
                        addBackingTableNoise: Boolean = false,
                        defaultProbaCriteria: DefaultProbaCriteriaConfig = DefaultProbaCriteriaConfig(),
                        windowSize: Option[Int] = None,
                        decimalDigits: Option[Int] = None) = {
    val percentiles = PyramidParser.readPercentileFile(percentileFile)
    val digitsToWrite: Int = decimalDigits match {
      case Some(i) => i
      case None => 4
    }

    val inputStream = CountTableLib.getInputStream(dataFile)
    val outWriter = CountTableLib.getOutputStream(outFile)

    val writeQueue = new LinkedBlockingQueue[String](bufferSize)
    val writeConsumer = new WriteConsumer(outWriter, writeQueue, bufferSize)
    val writeThread = new Thread(writeConsumer)
    writeThread.start()

    val vwIterator = new VWIterator(inputStream, bufferSize, bufferSize,
      percentiles, featureCount)

    var index = 0

    vwIterator.grouped(bufferSize).foreach { points =>

      val parsedOutLines: List[String] = points.zipWithIndex.par.map {
        case ((label: Double, weight: Option[Double], features: Array[Double], featureString: String), lineIndex: Int) =>
          val rawPoint = new LabeledPoint(label, features)

          val fullFeatureVectors = getFeatureVectors(featurizer,
            rawPoint,
            removeFirstLabel,
            featurizeWithCounts,
            featurizeWithTotalCount,
            featurizeWithProbabilites,
            addBackingTableNoise,
            defaultProbaCriteria,
            None
          )

          val featureVectors: ArrayBuffer[Array[Double]] = windowSize match {
            case None => fullFeatureVectors
            case Some(ws) => {
              val window = (lineIndex + index) / ws
              val windowFeatureVectors = getFeatureVectors(featurizer,
                rawPoint,
                removeFirstLabel,
                featurizeWithCounts,
                featurizeWithTotalCount,
                featurizeWithProbabilites,
                addBackingTableNoise,
                defaultProbaCriteria,
                Some(window)
              )
              fullFeatureVectors ++ windowFeatureVectors
            }
          }

          val incrBy = weight match {
            case Some(w) => w
            case None => 1.0
          }
          if (updateCountTable) {
            featurizer.addObservation(rawPoint, incrBy)
          }

          val featurizedLine = PyramidParser.getOutLine(
            rawPoint.label,
            featureVectors,
            digitsToWrite
          )

          val finalFeaturesString: String = if (!keepOriginalFeatures) {
            featurizedLine
          } else if (originalFeaturesToKeep.isEmpty) {
            featurizedLine.stripLineEnd + " |o " + featureString + "\n"
          } else {
            val featureStrings: Array[String] = featureString.split(" ")
            featurizedLine.stripLineEnd +
              " |o " +
              originalFeaturesToKeep.toArray.map { index => featureStrings(index) }.mkString(" ") +
              "\n"
          }
          finalFeaturesString
      }.toList

      parsedOutLines.foreach { outLine: String =>
        writeQueue.put(outLine)
      }

      println("Handling point: " + index)
      index += bufferSize
    }
    writeConsumer.end()
    writeThread.join()
    outWriter.close()
  }
    def getFeatureVectors(featurizer: PointFeaturization,
                        rawPoint: LabeledPoint,
                        removeFirstLabel: Boolean,
                        featurizeWithCounts: Boolean,
                        featurizeWithTotalCount: Boolean,
                        featurizeWithProbabilites: Boolean,
                        addBackingTableNoise: Boolean,
                        defaultProbaCriteria: DefaultProbaCriteriaConfig = DefaultProbaCriteriaConfig(),
                        window: Option[Int] = None):
  ArrayBuffer[Array[Double]] = {

    val featureVectors: ArrayBuffer[Array[Double]] =
      new ArrayBuffer[Array[Double]]()
    if (featurizeWithProbabilites) {
      val fwpVector =
        featurizer.featurizePoint(
          rawPoint,
          removeFirstLabel,
          false,
          false,
          true,
          addBackingTableNoise,
          defaultProbaCriteria=defaultProbaCriteria,
          window=window)
      require(fwpVector.length > 0)
      featureVectors.append(fwpVector)
    }

    if (featurizeWithCounts) {
      val fwcVector = featurizer.featurizePoint(
        rawPoint,
        removeFirstLabel,
        true,
        false,
        false,
        addBackingTableNoise,
        defaultProbaCriteria=defaultProbaCriteria,
        window=window
      )
      require(fwcVector.length > 0)
      featureVectors.append(fwcVector)
    }

    if (featurizeWithTotalCount) {
      val fwtcVector =
        featurizer.featurizePoint(
          rawPoint,
          removeFirstLabel,
          false,
          true,
          false,
          addBackingTableNoise,
          defaultProbaCriteria=defaultProbaCriteria,
          window=window
        )
      require(fwtcVector.length > 0)
      featureVectors.append(fwtcVector)
    }
    return featureVectors
  }
}
