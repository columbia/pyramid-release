package edu.columbia.cs.pyramid

import com.fasterxml.jackson.databind.JsonNode

class BuildOperation extends PyramidOperation {
  override val operationName: String = "build"

  override val pyramidConfigs = new PyramidConfigurations()
    // The number of features per observation
    .addIntConfiguration("featureCount", true)
    // Json array of all possible labels.
    .addDoubleSeqConfiguration("labels", true)
    // Name to be assigned to the count tables and featurizer. It's used when
    // storing count tables remotely.
    .addStringConfiguration("featurizerName", false, Some("featurizer"))
    // Nest array of features that should be counted together. The features are
    // interpretted as indexes started at 0.
    .addIntSeqSeqConfiguration("featureCombinations", true)
    // JSON object to configure the count table:
    // {"type": "unbiased_cms", "delta": 0.007, "epsilon": 0.0000272}
    // "type" may be cms, unbiased_cms, or exact. delta and epsilon configure
    // the size of the sketch.
    .addJsonNodeConfiguration("countTableConfig", true)
    // Set to true if the non private count table should be held in memory with
    // the noise added at query time.
    .addBoolConfiguration("isBackingNoise", false, Some(false))
    // The scale variable for the laplacian distribution.
    .addDoubleConfiguration("noiseLaplaceB", false, None)
    // The number of laplacian draws used to initialize the count tables.
    .addIntConfiguration("sumNNoiseDraws", false, Some(1))
    .addJsonNodeConfiguration("tableSpecificNumberOfQueries", false)
    .addJsonNodeConfiguration("dpNoiseShareWeights", false)
    .addJsonNodeConfiguration("featureImportanceWeights", false)
    .addJsonNodeConfiguration("perFeatureCountTableConfig", false)
    .addIntConfiguration("windowSize", false)
    .addJsonNodeConfiguration("dependentFeatures", false)
    .addStringConfiguration("percentilesFile", true)
    .addStringConfiguration("countDataFile", true)
    .addStringConfiguration("countTableFile", true)

  /**
    * Execute the associated operation with the configuration.
    */
  override def execute(jsonNode: JsonNode): Unit = {
    val parsedPyramidConfigs = pyramidConfigs.parseConfigurations(jsonNode)

    val instantiateCountTable = CountTableLib.getCountTableInstantiation(
      parsedPyramidConfigs.getJsonNodeConfiguration("countTableConfig"),
      parsedPyramidConfigs.getBoolConfiguration("isBackingNoise"),
      parsedPyramidConfigs.getOptionDoubleConfiguration("noiseLaplaceB"),
      parsedPyramidConfigs.getIntConfiguration("sumNNoiseDraws")
    )
    val instantiateLableCounter = CountTableLib.getLabelCounterInstantiation(
      parsedPyramidConfigs.getJsonNodeConfiguration("countTableConfig")
    )

    val tableSpecificNumberOfQueries =
      Some(CountTableLib.parseTableSpecificNumberOfQueries(
        parsedPyramidConfigs.getOptionJsonNodeConfiguration("tableSpecificNumberOfQueries")).toSeq)
    val tableSpecificNoiseWeights =
      parsedPyramidConfigs.getOptionDoubleSeqConfiguration("dpNoiseShareWeights")
    val featureImportanceWeights =
      parsedPyramidConfigs.getOptionDoubleSeqConfiguration("featureImportanceWeights")
    val tableSpecificLaplaceBMultipliers =
      CountTableLib.computeTableSpecificLaplaceBMultipliersSeq(
        tableSpecificNoiseWeights,
        featureImportanceWeights,
        tableSpecificNumberOfQueries
      )
    val tableSpecificLaplaceBs = parsedPyramidConfigs.getOptionDoubleConfiguration("noiseLapaceB") match {
      case Some(b) => tableSpecificLaplaceBMultipliers.map { (muls) =>
        muls.map { (mul) => mul * b }
      }
      case None => None
    }
    val tableSpecificInstantiations =
    (tableSpecificLaplaceBs, parsedPyramidConfigs.getOptionJsonNodeConfiguration("perFeatureCountTableConfig")) match {
      case (_, Some(node)) => CountTableLib.getCountTableSpecificInstantiations(
        CountTableLib.parsePerTableConfigs(node),
        parsedPyramidConfigs.getBoolConfiguration("isBackingNoise"),
        parsedPyramidConfigs.getOptionDoubleConfiguration("noiseLaplaceB"),
        tableSpecificLaplaceBs,
        parsedPyramidConfigs.getIntConfiguration("sumNNoiseDraws")
      )
      case (Some(bs), _) => CountTableLib.getCountTableSpecificInstantiations(
        bs.map((_) => parsedPyramidConfigs.getJsonNodeConfiguration("countTableConfig")),
        parsedPyramidConfigs.getBoolConfiguration("isBackingNoise"),
        parsedPyramidConfigs.getOptionDoubleConfiguration("noiseLaplaceB"),
        tableSpecificLaplaceBs,
          parsedPyramidConfigs.getIntConfiguration("sumNNoiseDraws")
      )
      case _ => null
    }
    val dependentFeatures =
      CountTableLib.parseDependentFeatures(parsedPyramidConfigs.getOptionJsonNodeConfiguration("dependentFeatures"))

    val featurizer = new Featurizer(
      parsedPyramidConfigs.getStringConfiguration("featurizerName"),
      parsedPyramidConfigs.getDoubleSeqConfiguration("labels"),
      parsedPyramidConfigs.getIntConfiguration("featureCount"),
      dependentFeatures,
      instantiateCountTable,
      instantiateLableCounter,
      tableSpecificInstantiations,
      tableSpecificLaplaceBs.getOrElse(null)
    )
    BuildCountTable.executeBuildCommand(featurizer,
      parsedPyramidConfigs.getStringConfiguration("percentilesFile"),
      parsedPyramidConfigs.getStringConfiguration("countDataFile"),
      parsedPyramidConfigs.getIntConfiguration("featureCount"),
      parsedPyramidConfigs.getDoubleSeqConfiguration("labels"),
      parsedPyramidConfigs.getStringConfiguration("countTableFile")
    )
  }

}
