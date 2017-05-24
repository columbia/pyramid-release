package edu.columbia.cs.pyramid

import java.io._

import com.fasterxml.jackson.databind.JsonNode

class DiscretizeOperation extends PyramidOperation {

  override val operationName: String = "discretize"

  override val pyramidConfigs = new PyramidConfigurations()
    // File containing quantile boundaries for continuous features.
    .addStringConfiguration("percentilelFile", true)
    // File containing the input data.
    .addStringConfiguration("dataFile", true)
    // File used to be used to write the discretized data.
    .addStringConfiguration("outFile", true)
    // The number of features.
    .addIntConfiguration("featureCount", true)

  /**
    * Execute the associated operation with the configuration.
    */
  override def execute(jsonNode: JsonNode): Unit = {
    val parsedPyramidConfigs = pyramidConfigs.parseConfigurations(jsonNode)
    discretizeDataFile(
      parsedPyramidConfigs.getStringConfiguration("percentileFile"),
      parsedPyramidConfigs.getStringConfiguration("dataFile"),
      parsedPyramidConfigs.getStringConfiguration("outFile"),
      parsedPyramidConfigs.getIntConfiguration("featureCount")
    )
  }

  def discretizeDataFile(percentileFile: String, dataFile: String,
                         outFile: String, featureCount: Int): Unit = {
    val percentiles = PyramidParser.readPercentileFile(percentileFile)
    val outWriter = new PrintWriter(new File(outFile))
    CountTableLib.getInputStream(dataFile).getLines().foreach { line =>
      val (label: Double, weight: Option[Double], features: Array[String]) =
        PyramidParser.parseDataFileLineToString(line, percentiles, featureCount)
      val outLine = label + " |f " + features.mkString(" ") + "\n"
      outWriter.write(outLine)
    }
    outWriter.close()
  }


}