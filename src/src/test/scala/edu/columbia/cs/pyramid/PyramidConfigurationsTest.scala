package edu.columbia.cs.pyramid

import com.fasterxml.jackson.core.JsonParser
import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import org.scalatest.{FunSuite, Matchers}


class PyramidConfigurationsTest extends FunSuite with Matchers {

  val jsonTestString = "{'testInt': 1, 'optionalDouble': 1.23, 'testDouble': 2.34, 'testSeq': [1,2,3], 'seqOfSeq':[[1,2],[3,4],[4]]}"
  val jsonNode = parseJson(jsonTestString)

  val pc = new PyramidConfigurations()
    .addIntConfiguration("testInt", true)
    .addDoubleConfiguration("testDouble", true)
    .addIntSeqConfiguration("testSeq", true)
    .addIntConfiguration("optionalInt", false, Some(10))
    .addDoubleConfiguration("optionalDouble", false, Some(12.1))
    .addIntSeqSeqConfiguration("seqOfSeq", true)
    .parseConfigurations(jsonNode)

  def parseJson(jsonString: String): JsonNode = {
    val objMapper = new ObjectMapper()
    objMapper.configure(JsonParser.Feature.ALLOW_COMMENTS, true)
    objMapper.configure(JsonParser.Feature.ALLOW_SINGLE_QUOTES, true)
    objMapper.registerModule(DefaultScalaModule)
    return objMapper.readTree(jsonString)
  }

  test("Ints should be parsed correctly") {
    val parsedInt: Double = pc.getIntConfiguration("testInt")
    parsedInt should equal(1)
  }

  test("Doubles should be parsed correctly") {
    val parsedDouble: Double = pc.getDoubleConfiguration("testDouble")
    parsedDouble should equal(2.34 +- 0.001)
  }

  test("Seqs should be parsed correctly") {
    val parsedSeq: Seq[Int] = pc.getIntSeqConfiguration("testSeq")
    parsedSeq(0) should equal(1)
    parsedSeq(1) should equal(2)
    parsedSeq(2) should equal(3)
  }

  test("OptionalInt should return default value") {
    val parsedInt: Int = pc.getIntConfiguration("optionalInt")
    parsedInt should equal(10)
  }

  test("OptionalDouble should return config value") {
    val parsedDouble: Double = pc.getDoubleConfiguration("optionalDouble")
    parsedDouble should equal(1.23 +- 0.001)
  }

  test("Seqs of Seqs should be parse correctly") {
    // [[1,2],[3,4],[4]]
    val seqOfSeq: Seq[Seq[Int]] = pc.getIntSeqSeqConfiguration("seqOfSeq")
    seqOfSeq.length should equal(3)
    seqOfSeq(0)(0) should equal(1)
    seqOfSeq(0)(1) should equal(2)

    seqOfSeq(1)(0) should equal(3)
    seqOfSeq(1)(1) should equal(4)

    seqOfSeq(2)(0) should equal(4)
  }

}
