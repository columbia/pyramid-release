package edu.columbia.cs.pyramid

import org.scalatest.{Matchers, FunSuite}

import scala.collection.mutable.ArrayBuffer


class OutlineTest extends FunSuite with Matchers {

  val fv1 = Array[Double](1.0, 2.0, 3.3, 0.000000002)
  val fv2 = Array[Double](3.4, 5.06, 2.121, 3, 4, 5, 6, 7, 8, 9)
  val fv3 = Array[Double](5, 1, 2, 3, 4, 5)
  val fv4 = Array[Double](1, 2, 1, 2, 3, 4, 5, 6, 4, 9)

  test("single namespace should start with one.") {
    val label = 1.0
    val featureVectors =
      ArrayBuffer[Array[Double]](fv1)
    val outline = PyramidParser.getOutLine(label, featureVectors, 4)
    outline should startWith("1.0 |1 0:1")
  }

  test("namespaces should be incremented correctly") {
    val label = 1.0
    val featureVectors =
      ArrayBuffer[Array[Double]](fv1, fv2, fv3, fv4)
    val outline = PyramidParser.getOutLine(label, featureVectors, 4)
    outline should include("|1 ")
    outline should include("|5")
    outline should include("|15")
    outline should include("|21")
  }
}
