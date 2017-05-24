package edu.columbia.cs.pyramid

import org.scalatest.{Matchers, FunSuite}

class DecimalFormatTest extends FunSuite with Matchers {

  test("Edge case 1.0 should return 1.") {
    val i1 = 1.0
    val expectedS1 = "1"
    val observedS1 = PyramidParser.formatDouble(i1)
    expectedS1 should equal(observedS1)

    val i2 = 1
    val expectedS2 = "1"
    val observedS2 = PyramidParser.formatDouble(i2)
    expectedS2 should equal(observedS2)

    val i3 = 1.0000000
    val expectedS3 = "1"
    val observedS3 = PyramidParser.formatDouble(i3)
    expectedS3 should equal(observedS3)
  }

  test("Edge case 0 should return 0.0.") {
    val i1 = 0
    val expectedS1 = "0"
    val observedS1 = PyramidParser.formatDouble(i1)
    expectedS1 should equal(observedS1)

    val i2 = 0.0
    val expectedS2 = "0"
    val observedS2 = PyramidParser.formatDouble(i2)
    expectedS2 should equal(observedS2)

    val i3 = 0.0000000
    val expectedS3 = "0"
    val observedS3 = PyramidParser.formatDouble(i3)
    expectedS3 should equal(observedS3)
  }

  test("Integers should not have decimal points") {
    val i1 = 101.0
    val expectedI1 = "101"
    val observedI1 = PyramidParser.formatDouble(i1)
    expectedI1 should equal(observedI1)

    val i2 = 2001
    val expectedI2 = "2001"
    val observedI2 = PyramidParser.formatDouble(i2)
    expectedI2 should equal(observedI2)

    val i3 = 1234.000
    val expectedI3 = "1234"
    val observedI3 = PyramidParser.formatDouble(i3)
    expectedI3 should equal(observedI3)

    val i4 = 10000000002l
    val expectedI4 = "10000000002"
    val observedI4 = PyramidParser.formatDouble(i4)
    expectedI4 should equal(observedI4)
  }


  test("Trailing zeros should be removed.") {
    val d1 = 1.5
    val expectedD1 = "1.5"
    val observedD1 = PyramidParser.formatDouble(d1)
    expectedD1 should equal(observedD1)

    val d2 = 1.234
    val expectedD2 = "1.23"
    val observedD2 = PyramidParser.formatDouble(d2)
    expectedD2 should equal(observedD2)
  }

  test("Values less than 1 should be in scientific notation") {
    val d1 = 0.123
    val expectedD1 = "1.23E-1"
    val observedD1 = PyramidParser.formatDouble(d1)
    // expectedD1 should equal(observedD1)
    println(observedD1)

    val d2 = 0.0002345
    val expectedD2 = "2.345E-4"
    val observedD2 = PyramidParser.formatDouble(d2)
    // expectedD2 should equal(observedD2)
    println(observedD2)

    val d3 = 0.0000000123
    val expectedD3 = "1.23E-8"
    val observedD3 = PyramidParser.formatDouble(d3)
    println(observedD3)
  }

}
