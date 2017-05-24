package edu.columbia.cs.pyramid


import java.io._
import java.util.zip.{GZIPOutputStream, GZIPInputStream}

import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import com.twitter.chill.{Input, Output, ScalaKryoInstantiator}

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.io.{BufferedSource, Source}

object CountTableLib {
  def isGzFile(filePath: String): Boolean = {
    val s1: Byte = 0x1f.toByte
    val s2: Byte = 0x8b.toByte

    val pb = new PushbackInputStream(new FileInputStream(filePath), 2)
    val signature = new Array[Byte](2)
    pb.read(signature)
    pb.close()

    return signature(0) == s1 && signature(1) == s2
  }

  def getInputStream(filePath: String): BufferedSource = {
    if (isGzFile(filePath)) {
      return Source.fromInputStream(
        new GZIPInputStream(
          new BufferedInputStream(
            new FileInputStream((filePath)))))
    } else {
      return Source.fromFile(filePath)
    }

  }

  def getOutputStream(filePath: String): PrintWriter = {
    return new PrintWriter(new GZIPOutputStream(
      new BufferedOutputStream(
        new FileOutputStream(filePath))))
  }

  def parseLabels(labelNode: JsonNode): Seq[Double] = {
    var returnValue = IndexedSeq[Double]()
    require(labelNode.isArray)
    var index = 0
    while (true) {
      if (labelNode.get(index) == null) {
        return returnValue
      }
      val currentLabel = labelNode.get(index).asDouble()
      returnValue = returnValue :+ currentLabel
      index += 1
    }
    return returnValue
  }

  def parseDependentFeatures(depFeatureNode: Option[JsonNode]): Seq[Seq[Int]] = depFeatureNode match {
    case Some(node) => parseDependentFeatures(node)
    case None => mutable.IndexedSeq[Seq[Int]]()
  }

  def parseDependentFeatures(depFeatureNode: JsonNode): Seq[Seq[Int]] = {
    var depFeatures = IndexedSeq[Seq[Int]]()
    require(depFeatureNode.isArray)
    var index = 0
    while (true) {
      if (depFeatureNode.get(index) == null) {
        return depFeatures
      }
      val featureGroupNode = depFeatureNode.get(index)
      val featureGroup = parseLabels(featureGroupNode).map(i => i.toInt)
      depFeatures = depFeatures :+ featureGroup
      index += 1
    }
    return depFeatures
  }

  def parseTableSpecificNumberOfQueries(node: Option[JsonNode]): Array[Int] = node match {
    case Some(n) => parseTableSpecificNumberOfQueries(n)
    case None => Array[Int]()
  }

  def parseTableSpecificNumberOfQueries(node: JsonNode): Array[Int] = {
    require(node.isArray)

    var queriesN = IndexedSeq[Int]()
    var index = 0
    while (true) {
      if (node.get(index) == null) return queriesN.toArray
      val currentQueriesN = node.get(index).asInt
      queriesN = queriesN :+ currentQueriesN
      index += 1
    }
    return queriesN.toArray
  }

  def parseDpNoiseShareWeights(dpNoiseShareWeightsNode: JsonNode): Array[Double] = {
    require(dpNoiseShareWeightsNode.isArray || dpNoiseShareWeightsNode.isTextual)

    val weightsArray = if (dpNoiseShareWeightsNode.isTextual) {
      readWeightsFile(dpNoiseShareWeightsNode.asText)
    } else {
      var weights = IndexedSeq[Double]()
      var index = 0
      while (true) {
        if (dpNoiseShareWeightsNode.get(index) == null) return weights.toArray
        val currentWeight = dpNoiseShareWeightsNode.get(index).asDouble
        weights = weights :+ currentWeight
        index += 1
      }
      weights.toArray
    }
    return weightsArray
  }

  def parseFeatureImportanceWeights(featureImportanceWeightsNode: JsonNode): Array[Double] = {
    require(featureImportanceWeightsNode.isArray || featureImportanceWeightsNode.isTextual)

    val weightsArray = if (featureImportanceWeightsNode.isTextual) {
      readWeightsFile(featureImportanceWeightsNode.asText)
    } else {
      var weights = IndexedSeq[Double]()
      var index = 0
      while (true) {
        if (featureImportanceWeightsNode.get(index) == null) return weights.toArray
        val currentWeight = featureImportanceWeightsNode.get(index).asDouble
        weights = weights :+ currentWeight
        index += 1
      }
      weights.toArray
    }
    return weightsArray
  }

  def computeTableSpecificLaplaceBMultipliersSeq(dpNoiseShareWeigths: Option[Seq[Double]],
                                       featureImportanceWeights: Option[Seq[Double]],
                                       tableSpecificNumberOfQueries: Option[Seq[Int]]):
                                       Option[Array[Double]] = {
    val dpnsw = dpNoiseShareWeigths match {
      case Some(s) => Some(s.toArray)
      case None => None
    }
    val fiw = featureImportanceWeights match {
      case Some(s) => Some(s.toArray)
      case None => None
    }
    val tsnq = tableSpecificNumberOfQueries match {
      case Some(s) => Some(s.toArray)
      case None => None
    }
    return computeTableSpecificLaplaceBMultipliers(dpnsw, fiw, tsnq)
  }

  def computeTableSpecificLaplaceBMultipliers(dpNoiseShareWeigths: Option[Array[Double]],
                                       featureImportanceWeights: Option[Array[Double]],
                                       tableSpecificNumberOfQueries: Option[Array[Int]]):
                                       Option[Array[Double]] = {
    val weightsArray = (dpNoiseShareWeigths, featureImportanceWeights) match {
      case (Some(noiseShare), Some(featureImportance)) => {
        noiseShare.zip(featureImportance).map { case (nS, fI) => nS / fI }
      }
      case (Some(noiseShare), None)                    => noiseShare
      case (None, Some(featureImportance))             => {
        featureImportance.map { fi => 1 / fi }
      }
      case (None, None)                                => return None
    }
    val len = weightsArray.length
    val queriesN: Array[Int] = tableSpecificNumberOfQueries match {
      case Some(a) => a
      case None    => Array.fill[Int](len)(1)
    }
    val totQueries = queriesN.sum
    // 1 + x in case x is 0
    val inverseWeightsArray = weightsArray.map { (x) => 1 / (1 + x) }
    println(inverseWeightsArray.mkString(", "))
    val tot = inverseWeightsArray.zip(queriesN).map { case (w, queryN) => w * queryN }.sum
    val epsilonFraction = inverseWeightsArray.zip(queriesN).map { case (w, queryN) => queryN * w / tot }
    println(epsilonFraction.mkString(", "))
    val multipliers = epsilonFraction.map { (ef) => 1 / (ef * totQueries) }
    println(multipliers.mkString(", "))
    return Some(multipliers)
  }

  def readWeightsFile(weigthsFile: String): Array[Double] = {
    return scala.io.Source.fromFile(new File(weigthsFile)).getLines().map { line =>
      line.toDouble
    }.toArray
  }

  def parsePerFeatureNoise(perFeatureNoiseNode: JsonNode): Array[Double] = {
    var noises = IndexedSeq[Double]()
    require(perFeatureNoiseNode.isArray)
    var index = 0
    while (true) {
      if (perFeatureNoiseNode.get(index) == null) return noises.toArray
      val currentNoiseB = perFeatureNoiseNode.get(index).asDouble()
      noises = noises :+ currentNoiseB
      index += 1
    }
    return noises.toArray
  }

  def parsePerTableConfigs(perTableConfigNode: JsonNode): Array[JsonNode] = {
    var configs = IndexedSeq[JsonNode]()
    require(perTableConfigNode.isArray)
    var index = 0
    while (true) {
      if (perTableConfigNode.get(index) == null) return configs.toArray
      configs = configs :+ perTableConfigNode.get(index)
      index += 1
    }
    return configs.toArray
  }

  def getExactTableInstantiation(configNode: JsonNode,
                                 isBackingNoise: Boolean,
                                 noiseLaplaceB: Option[Double],
                                 sumNNoiseDraws: Int):
  (String, Option[Double]) => CountTable = {
    def createExactCountTable(name: String, laplaceB: Option[Double]): CountTable = {
      val b = if(laplaceB == None) noiseLaplaceB else laplaceB
      // println("create table: " + name + " with noise : " + b + " and sumNNoiseDraws: " + sumNNoiseDraws)
      return new ExactCountTable(isBackingNoise, b, sumNNoiseDraws)
    }
    return createExactCountTable
  }

  def getCMSInstantiation(configNode: JsonNode,
                          isBackingNoise: Boolean,
                          noiseLaplaceB: Option[Double],
                          sumNNoiseDraws: Int):
  (String, Option[Double]) => CountTable = {
    require(configNode.has("epsilon"))
    require(configNode.has("delta"))

    val epsilon = configNode.get("epsilon").asDouble()
    val delta = configNode.get("delta").asDouble()

    val depth = CountTable.getDepth(delta)
    val seeds = Some((0 until depth).toArray)

    def createApproximateCountTable(name: String, laplaceB: Option[Double]): CountTable = {
      val b = if(laplaceB == None) noiseLaplaceB else laplaceB
      // println("create table: " + name + " with noise : " + b + " and sumNNoiseDraws: " + sumNNoiseDraws)
      return new ApproximateCountTable(epsilon, delta,
        isBackingNoise, b, sumNNoiseDraws, seeds)
    }
    return createApproximateCountTable
  }

  def getUnbiasedCMSInstantiation(configNode: JsonNode,
                                  isBackingNoise: Boolean,
                                  noiseLaplaceB: Option[Double],
                                  sumNNoiseDraws: Int):
  (String, Option[Double]) => CountTable = {
    require(configNode.has("epsilon"))
    require(configNode.has("delta"))

    val epsilon = configNode.get("epsilon").asDouble()
    val delta = configNode.get("delta").asDouble()

    val depth = CountTable.getDepth(delta)
    val seeds = Some((0 until depth*2).toArray)

    def createApproximateCountTable(name: String, laplaceB: Option[Double]): CountTable = {
      val b = if(laplaceB == None) noiseLaplaceB else laplaceB
      // println("create unbiased table: " + name + " with noise: " + b + " and sumNNoiseDraws: " + sumNNoiseDraws)
      return new UnbiasedApproximateCountTable(epsilon, delta,
        isBackingNoise, b, sumNNoiseDraws, seeds)
    }
    return createApproximateCountTable
  }

  def getCountTableInstantiation(configNode: JsonNode,
                                 isBackingNoise: Boolean,
                                 noiseLaplaceB: Option[Double],
                                 sumNNoiseDraws: Int = 1):
  (String, Option[Double]) => CountTable = {
    require(configNode.has("type"))
    val countTableType = configNode.get("type").asText
    return countTableType match {
      case "exact" =>
        getExactTableInstantiation(configNode, isBackingNoise, noiseLaplaceB, sumNNoiseDraws)
      case "cms" =>
        getCMSInstantiation(configNode, isBackingNoise, noiseLaplaceB, sumNNoiseDraws)
      case "unbiased_cms" =>
        getUnbiasedCMSInstantiation(configNode, isBackingNoise, noiseLaplaceB, sumNNoiseDraws)
      case _ => throw new IllegalArgumentException(
        "Unrecognized count table type: " + countTableType)
    }
  }

  def getCountTableSpecificInstantiations(configNodes: Array[JsonNode],
                                          isBackingNoise: Boolean,
                                          noiseLaplaceB: Option[Double],
                                          tableSpecificLaplaceBs: Option[Array[Double]],
                                          sumNNoiseDraws: Int = 1):
  Array[(String, Option[Double]) => CountTable] = {
    println(tableSpecificLaplaceBs.mkString(","))
    configNodes.zipWithIndex.map { case (x, i) =>
      println("x: " + x)
      val laplaceB = (noiseLaplaceB, tableSpecificLaplaceBs) match {
        case (_, Some(bs))   => Some(bs(i))
        case (Some(b), None) => Some(b)
        case               _ => None
      }
      getCountTableInstantiation(x, isBackingNoise, laplaceB, sumNNoiseDraws)
    }
  }

  def instantiateLocalLabelCounter(name: String): LabelCounter = {
    return new LocalLabelCounter()
  }

  def getLabelCounterInstantiation(configNode: JsonNode): (String) => LabelCounter = {
    require(configNode.has("type"))
    val countTableType = configNode.get("type").asText
    return countTableType match {
      case "cms" => instantiateLocalLabelCounter
      case "unbiased_cms" => instantiateLocalLabelCounter
      case "precise" => instantiateLocalLabelCounter
      case _ => throw new IllegalArgumentException(
        "Unrecognized count table type: " + countTableType)
    }
  }

  def serializeHotWindowManager(hwm: HotWindowManager, outPath: String): Unit = {
    val countFileOutput = new Output(new FileOutputStream(outPath))
    val kryoInstantiator = new ScalaKryoInstantiator
    kryoInstantiator.setRegistrationRequired(true)
    val kryo = kryoInstantiator.newKryo()
    kryo.register(classOf[HotWindowManager])
    hwm.cleanup()
    kryo.writeObject(countFileOutput, hwm)
    countFileOutput.close()
  }

  def deserializeHotWindowManager(inPath: String):
  HotWindowManager = {

    val kryoInstantiator = new ScalaKryoInstantiator
    kryoInstantiator.setRegistrationRequired(true)
    val kryo = kryoInstantiator.newKryo()
    kryo.register(classOf[HotWindowManager])
    val input = new Input(new FileInputStream(inPath))
    val newHWM = kryo.readObject(input, classOf[HotWindowManager])
    input.close()
    return newHWM
  }

  def serializeWindowedFeaturizer(wf: WindowedFeaturizer,
                                  outPath: String): Unit = {
    val countFileOutput = new Output(new FileOutputStream(outPath))
    val kryoInstantiator = new ScalaKryoInstantiator
    kryoInstantiator.setRegistrationRequired(true)
    val kryo = kryoInstantiator.newKryo()
    kryo.register(classOf[WindowedFeaturizer])
    wf.cleanup()
    kryo.writeObject(countFileOutput, wf)
    countFileOutput.close()
  }

  def deserializeWindowedFeaturizer(inPath: String): WindowedFeaturizer = {

    val kryoInstantiator = new ScalaKryoInstantiator
    kryoInstantiator.setRegistrationRequired(true)
    val kryo = kryoInstantiator.newKryo()
    kryo.register(classOf[HotWindowManager])
    val input = new Input(new FileInputStream(inPath))
    val wf = kryo.readObject(input, classOf[WindowedFeaturizer])
    input.close()
    return wf
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
