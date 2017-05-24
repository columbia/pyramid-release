package edu.columbia.cs.pyramid

import scala.StringBuilder
import scala.collection.mutable
import scala.util.hashing.MurmurHash3

/**
  * Abstraction for a feature vector that will handle maintaining namespaces
  * and n-gram features.
  *
  * @param data
  * @param bitPrecision
  * @param ngramNamespaces
  */
class FeatureSpace(data: Map[String, IndexedSeq[(String, Double)]],
                   bitPrecision: Int,
                   ngramNamespaces: Map[String, Int] = Map[String, Int]()) {

  val bitSize = math.pow(2, bitPrecision).toInt

  val namespaceDelimiter = "^"

  def getFeatureSet(): Seq[(String, Double)] = {
    val ngramKeySet = ngramNamespaces.keySet
    return data.map {
      case (namespace: String, features: Seq[(String, Double)]) =>
        ngramKeySet.contains(namespace) match {
          case true => getNGramFeatureSet(namespace, features, ngramNamespaces.getOrElse(namespace, 0))
          case false => getFeatureSet(namespace, features)
        }
    }.flatten.toSeq
  }

  def getNGramFeatureSet(namespace: String,
                         features: IndexedSeq[(String, Double)],
                         n: Int): Seq[(String, Double)] = {
    require(n > 0)
    val featureStrings: Seq[String] =
      features.map(sd => sd._2.toString)
    val featureLength = features.length
    return (0 until featureLength).map { i =>
      val baseIndex = i
      val upperIndex = math.min(featureLength, baseIndex + n)
      val featureSlice = featureStrings.slice(baseIndex, upperIndex)
      val valueKey = getDoubleKey(namespace, featureSlice)
      (namespace, valueKey)
    }
  }

  def getFeatureSet(namespace: String,
                    features: Seq[(String, Double)]): Seq[(String, Double)] = {
    return features.map {
      case (featureKey: String, featureValue: Double) =>
        (getKeyString(namespace, featureKey), featureValue)
    }
  }

  /**
    * Creates a byte array key from the namespace and the feature.
    *
    * @param namespace
    * @param featureKey
    * @return
    */
  def getKeyString(namespace: String, featureKey: String): String = {
    val sb = new StringBuilder()
    return sb.append(namespace).append(namespaceDelimiter)
      .append(featureKey).toString()
  }

  /**
    * Creates a keystring by appending the feature keys and namespace with
    * delimiters.
    *
    * @param namespace
    * @param featureKeys
    * @return
    */
  def getKeyString(namespace: String, featureKeys: Seq[String]): String = {
    var sb = new StringBuilder(namespace).append(namespaceDelimiter)
    for (fk: String <- featureKeys) {
      sb = sb.append(fk).append(namespaceDelimiter)
    }
    return sb.toString()
  }

  /**
    * Creates a key from the namespace and feature key by hashing the byte
    * array of the concatenated values.
    *
    * @param namespace
    * @param featureKey
    * @return
    */
  def getDoubleKey(namespace: String, featureKey: Seq[String]): Double = {
    val key = getKeyString(namespace, featureKey)
    return nonNegativeMod(MurmurHash3.stringHash(key), bitSize).toDouble
  }

  def nonNegativeMod(base: Int, mod: Int): Int = {
    require(mod > 0)
    return (base & Int.MaxValue) % mod
  }

}
