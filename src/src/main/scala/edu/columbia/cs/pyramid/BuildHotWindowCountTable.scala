package edu.columbia.cs.pyramid

import com.fasterxml.jackson.databind.JsonNode

/**
  * TODO: I don't know if we still use this. Need to figure that out. Commenting
  * out the broken section for now.
  */

object BuildHotWindowCountTable {


  def getInstantiateFeaturizer(labels: Seq[Double],
                               featureCount: Int,
                               dependentFeatures: Seq[Seq[Int]],
                               getCountTable: (String, Option[Double]) => CountTable,
                               getLabelCounter: (String) => LabelCounter):
  (String) => Featurizer = {
    def instantiateFeaturizer(name: String): Featurizer = {
      return new Featurizer(name, labels, featureCount, dependentFeatures,
        getCountTable, getLabelCounter)
    }
    return instantiateFeaturizer
  }

  def executeBuildHotWindowCountTable(configNode: JsonNode, args: Array[String]): Unit = {
    if (args.length != 3) {
      println("Build command requires <percentileFile> <countFile> <data file>")
      sys.exit(-1)
    }

    require(configNode.has("featureCount"))
    require(configNode.has("labels"))
    require(configNode.has("featureCombinations"))
    require(configNode.get("labels").isArray())
    require(configNode.has("countTableConfig"))
    require(configNode.has("hotWindowSize"))
    require(configNode.has("countTableSize"))

    // Noise for each window
    val windowsIsBackingNoise = configNode.get("windowsIsBackingNoise") match {
      case node: JsonNode => node.asBoolean()
      case _ => false
    }
    val windowNoiseLaplaceB = configNode.get("windowNoiseLaplaceB") match {
      case node: JsonNode => Some(1.0 / node.asDouble)
      case _ => None
    }

    // Noise for the master window.
    val masterIsBackingNoise = configNode.get("masterIsBackingNoise") match {
      case node: JsonNode => node.asBoolean()
      case _ => false
    }
    val masterNoiseLaplaceB = configNode.get("masterLaplaceB") match {
      case node: JsonNode => Some(1.0 / node.asDouble)
      case _ => None
    }

    // Initialization noise for the master count table.
    val masterInitNoiseEpsilon = configNode.get("masterInitLaplacianSD") match {
      case node: JsonNode => Some(1.0 / node.asDouble)
      case _ => None
    }

    val countTableCount: Double = configNode.get("countTableCount") match {
      case node: JsonNode => 1.0 / node.asDouble
      case _ => Double.PositiveInfinity
    }

    val hotWindowSize = configNode.get("hotWindowSize").asInt
    val countTableSize = configNode.get("countTableSize").asInt

    val labelNode = configNode.get("labels")
    val labels = CountTableLib.parseLabels(labelNode)
    val depFeatureNode = configNode.get("featureCombinations")
    val dependentFeatures = CountTableLib.parseDependentFeatures(depFeatureNode)
    val featureCount = configNode.get("featureCount").asInt
    val featurizerName = configNode.get("name") match {
      case node: JsonNode => node.asText
      case _ => "featurizer"
    }

    val instantiateWindowCountTable = CountTableLib.getCountTableInstantiation(
      configNode.get("countTableConfig"), windowsIsBackingNoise,
      windowNoiseLaplaceB)
    val instantiateMasterCountTable = CountTableLib.getCountTableInstantiation(
      configNode.get("countTableConfig"), masterIsBackingNoise,
      masterNoiseLaplaceB)


    val instantiateLabelCounter = CountTableLib.getLabelCounterInstantiation(
      configNode.get("countTableConfig"))
    val countTableIsThreadSafe: Boolean = configNode
      .get("countTableConfig")
      .get("threadSafe") match {
      case node: JsonNode => node.asBoolean
      case _ => false
    }

    val instantiateWindowFeaturizer = getInstantiateFeaturizer(labels,
      featureCount, dependentFeatures, instantiateWindowCountTable,
      instantiateLabelCounter)
    val instantiateMasterFeaturizer = getInstantiateFeaturizer(labels,
      featureCount, dependentFeatures, instantiateMasterCountTable,
      instantiateLabelCounter)

    val localHotWindowCollection = new LocalHotWindowCollection()

    val hotWindowManager = new HotWindowManager(hotWindowSize, countTableCount,
      countTableSize, None, localHotWindowCollection,
      instantiateWindowFeaturizer, instantiateMasterFeaturizer)

    val percentileFile = args(0)
    val countDataFile = args(1)
    val countTableFile = args(2)
    BuildCountTable.buildCountTable(hotWindowManager,
      percentileFile, countDataFile, featureCount, labels,
      countTableIsThreadSafe)
    CountTableLib.serializeHotWindowManager(hotWindowManager, countTableFile)
  }

  def executeTransformCommand(configNode: JsonNode, args: Array[String]): Unit = {
    if (args.length != 4 && args.length != 5) {
      println("Transform command requires <percentile> <countFile> <dataFile> <outFile> [updatedCountTable]")
      sys.exit(-1)
    }
    require(configNode.has("featureCount"))

    val shouldAddBackingTableNoise = configNode.get("backingTableNoise") match {
      case n: JsonNode => n.asBoolean
      case _ => false
    }

    val removeFirstLabel = configNode.get("removeFirstLabel") match {
      case b: JsonNode => b.asBoolean
      case _ => false
    }
    val keepOriginalFeatures = configNode.get("keepOriginalFeatures") match {
      case b: JsonNode => b.asBoolean
      case _ => false
    }
    val originalFeaturesToKeep = configNode.get("originalFeaturesToKeep") match {
      case n: JsonNode => CountTableLib.parseLabels(n).map(_.toInt)
      case _ => Seq[Int]()
    }
    val featurizeWithCounts = configNode.get("featurizeWithCounts") match {
      case n: JsonNode => n.asBoolean
      case _ => false
    }
    val featurizeWithTotalCount = configNode.get("featurizeWithTotalCount") match {
      case n: JsonNode => n.asBoolean
      case _ => false
    }
    val featurizeWithProbabilities = configNode.get("featurizeWithProbabilities") match {
      case n: JsonNode => n.asBoolean
      case _ => true
    }

    val newCountTablePath: Option[String] = args.length match {
      case 5 => Some(args(4))
      case _ => None
    }

    val shouldWriteNewCountTable: Boolean = args.length match {
      case 5 => true
      case _ => false
    }


    require(featurizeWithCounts || featurizeWithProbabilities)

    val percentilesFile = args(0)
    val countTableFile = args(1)
    val inputDataFile = args(2)
    val outDataFile = args(3)

    /*
    val hwm = CountTableLib.deserializeHotWindowManager(countTableFile)
    BuildCountTable.transformDataFile(
      hwm.getMergedWindow(true),
      percentilesFile,
      inputDataFile,
      outDataFile,
      configNode.get("featureCount").asInt,
      removeFirstLabel,
      keepOriginalFeatures,
      originalFeaturesToKeep,
      featurizeWithCounts,
      featurizeWithTotalCount,
      featurizeWithProbabilities,
      updatedCountFile = newCountTablePath,
      updateCountTable = shouldWriteNewCountTable,
      addBackingTableNoise = shouldAddBackingTableNoise
    )

    newCountTablePath match {
      case Some(ucf) => {
        println("Serializing updated featurizer")
        CountTableLib.serializeHotWindowManager(hwm, ucf)
      }
      case None => println("Not writing updated featurizer")
    }
  */
  }
}
