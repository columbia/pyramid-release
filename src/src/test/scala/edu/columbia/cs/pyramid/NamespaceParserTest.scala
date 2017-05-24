package edu.columbia.cs.pyramid

import org.scalatest.{Matchers, FunSuite}

class NamespaceParserTest extends FunSuite with Matchers {
  val percentiles = Map[Int, Array[Double]]()

  test("line with no namespaces hould have one default key") {
    val line = "1 | 1 2 3 4"
    val (label: Double, features: Map[String, Seq[(String, Double)]]) =
      NamespacedParser.parseDataLine(line, percentiles)
    features.size should equal(1)
    features.keys.head should equal(NamespacedParser.defaultNamespace)
  }

  test("line with one namespaces hould have one key") {
    val line = "1 |abc 1 2 4:3 a:1"
    val (label: Double, features: Map[String, Seq[(String, Double)]]) =
      NamespacedParser.parseDataLine(line, percentiles)

    features.size should equal(1)
    features.keys.head should equal("abc")
  }

  test("line with multiple default namespaces should be consolidated") {
    val line = "1 | 1 2 3 4 | a b c 1 2 "
    val (label: Double, features: Map[String, Seq[(String, Double)]]) =
      NamespacedParser.parseDataLine(line, percentiles)
    features.size should equal(1)
    features.keys.head should equal(NamespacedParser.defaultNamespace)
  }

  test("line wiht multiple non-default namespaces should be consolidated") {
    val line = "1 |ns1 1 2 3 4 |ns1 a b c 1 2 "
    val (label: Double, features: Map[String, Seq[(String, Double)]]) =
      NamespacedParser.parseDataLine(line, percentiles)
    features.size should equal(1)
    features.keys.head should equal("ns1")
  }

  test("line with multiple namespace should be not be consolidated") {
    val line = "1 |ns1 1 2 3 4 |ns1 a b c 1 2 | a b a c |ns2 a b f"
    val (label: Double, features: Map[String, Seq[(String, Double)]]) =
      NamespacedParser.parseDataLine(line, percentiles)
    features.size should equal(3)
    val ks = features.keySet
    ks.contains("ns1") should be(true)
    ks.contains(NamespacedParser.defaultNamespace) should be(true)
    ks.contains("ns2") should be(true)
  }

  test("namespace with mixture of keyed and plain features should mix correctly") {
    val line = "1 |ns1 1 a:2 5 c:5 |ns1 6 7 8"
    val (label: Double, features: Map[String, Seq[(String, Double)]]) =
      NamespacedParser.parseDataLine(line, percentiles)
    features.get("ns1") match {
      case Some(features) =>{
        val featureMap = features.toMap
        featureMap.getOrElse("0", 0.0) should equal(1.0)
        featureMap.getOrElse("a", 0.0) should equal(2.0)
        featureMap.getOrElse("2", 0.0) should equal(5.0)
        featureMap.getOrElse("c", 0.0) should equal(5.0)
        featureMap.getOrElse("4", 0.0) should equal(6.0)
        featureMap.getOrElse("5", 0.0) should equal(7.0)
        featureMap.getOrElse("6", 0.0) should equal(8.0)
      }
      case None =>
        throw new IllegalArgumentException("Namespace ns1 should exist")
    }
  }

  test("namespace with mixture of keyed and plain of alphanumeric features should mix correctly") {
    val line = "1 |ns1 aff a:2 5 c:5 |ns1 d 7 abc"
    val (label: Double, features: Map[String, Seq[(String, Double)]]) =
      NamespacedParser.parseDataLine(line, percentiles)
    features.get("ns1") match {
      case Some(features) =>{
        val featureMap = features.toMap
        featureMap.getOrElse("0", 0.0) should equal("aff".hashCode)
        featureMap.getOrElse("a", 0.0) should equal(2.0)
        featureMap.getOrElse("2", 0.0) should equal(5.0)
        featureMap.getOrElse("c", 0.0) should equal(5.0)
        featureMap.getOrElse("4", 0.0) should equal("d".hashCode)
        featureMap.getOrElse("5", 0.0) should equal(7.0)
        featureMap.getOrElse("6", 0.0) should equal("abc".hashCode)
      }
      case None =>
        throw new IllegalArgumentException("Namespace ns1 should exist")
    }
  }

  test("multiple namespaces with mixture of keyed and plain of alphanumeric features should mix correctly") {
    val line = "1 |ns1 aff a:2 5 c:5 |ns2 d 7 abc"
    val (label: Double, features: Map[String, Seq[(String, Double)]]) =
      NamespacedParser.parseDataLine(line, percentiles)
    features.get("ns1") match {
      case Some(features) =>{
        val featureMap = features.toMap
        featureMap.getOrElse("0", 0.0) should equal("aff".hashCode)
        featureMap.getOrElse("a", 0.0) should equal(2.0)
        featureMap.getOrElse("2", 0.0) should equal(5.0)
        featureMap.getOrElse("c", 0.0) should equal(5.0)
      }
      case None =>
        throw new IllegalArgumentException("Namespace ns1 should exist")
    }
    features.get("ns2") match {
      case Some(features) =>{
        val featureMap = features.toMap
        featureMap.getOrElse("0", 0.0) should equal("d".hashCode)
        featureMap.getOrElse("1", 0.0) should equal(7.0)
        featureMap.getOrElse("2", 0.0) should equal("abc".hashCode)
      }
      case None =>
        throw new IllegalArgumentException("Namespace ns1 should exist")
    }
  }
}
