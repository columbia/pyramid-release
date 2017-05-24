package edu.columbia.cs.pyramid

import org.scalatest.{Matchers, FunSuite}

class FeatureSpaceTest extends FunSuite with Matchers {
  val nGramData = IndexedSeq[(String, Double)](
    ("0", "a".hashCode),
    ("1", "aa".hashCode),
    ("2", "aaa".hashCode),
    ("3", "aaaa".hashCode),
    ("4", "aaaaa".hashCode),
    ("5", "aaaaaa".hashCode),
    ("6", "aaaaaaa".hashCode),
    ("7", "aaaaaaaa".hashCode),
    ("8", "aaaaaaaaa".hashCode),
    ("9", "aaaaaaaaa".hashCode),
    ("10", "aaaaaaaaaa".hashCode))
  val defaultData = IndexedSeq[(String, Double)](
    ("a", 1.0),
    ("b", 12.0),
    ("c", 15.0))
  val dataSet = Seq[(String, IndexedSeq[(String, Double)])](
    ("ns1", nGramData),
    ("ns2", defaultData)).toMap
  val bitPrecision = 28

  test("data with no Ngram features should be correct.") {
    val featureSpace = new FeatureSpace(dataSet, bitPrecision)
    val fs = featureSpace.getFeatureSet().toMap
    val fsKeySet = fs.keySet
    fsKeySet.size should equal(14)
    nGramData.foreach {
      case (key: String, value: Double) =>
        val nsKey = new StringBuilder("ns1")
          .append(featureSpace.namespaceDelimiter) .append(key).toString()
        fsKeySet should contain(nsKey)
        fs.getOrElse(nsKey, 0.0) should equal(value)
    }
    defaultData.foreach {
      case (key: String, value: Double) =>
        val nsKey = new StringBuilder("ns2")
          .append(featureSpace.namespaceDelimiter).append(key).toString()
        fsKeySet should contain(nsKey)
        fs.getOrElse(nsKey, 0.0) should equal(value)
    }
  }
  test("data with no 1-gram features should be correct.") {
    val ngramSpaces = Map[String, Int]("ns1" -> 1)
    val featureSpace = new FeatureSpace(dataSet, bitPrecision, ngramSpaces)
    val fs = featureSpace.getFeatureSet()
    fs.length should be(14)
  }

  test("data with no 2-gram features should be correct.") {
    val ngramSpaces = Map[String, Int]("ns1" -> 2)
    val featureSpace = new FeatureSpace(dataSet, bitPrecision, ngramSpaces)
    val fs = featureSpace.getFeatureSet()
    fs.length should be(14)
  }

  test("data with no 3-gram features should be correct.") {
    val ngramSpaces = Map[String, Int]("ns1" -> 3)
    val featureSpace = new FeatureSpace(dataSet, bitPrecision, ngramSpaces)
    val fs = featureSpace.getFeatureSet()
    fs.length should be(14)
  }
}
