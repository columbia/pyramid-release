package edu.columbia.cs.pyramid

import org.scalatest.{FunSuite, Matchers}

class LocalHotWindowCollectionTest extends FunSuite with Matchers {

  val testPoint = new LabeledPoint(1.0, Array(1, 2, 3))
  val testWeight = 1.0
  val testPointWeight = (testPoint, testWeight)

  test("Local hot window collection should return the correct length.") {
    val lhwc = new LocalHotWindowCollection()

    lhwc.length should be(0)

    val insertions = 100
    for (i <- 0 until insertions) {
      lhwc.push(testPointWeight)
    }

    lhwc.length should be(insertions)
  }

  test("should throw an exception when popping more elements than are in the " +
    "hot window") {
    val lhwc = new LocalHotWindowCollection()
    an[NoSuchElementException] should be thrownBy lhwc.pop(1)

    val insertions = 100
    for (i <- 0 until insertions) {
      lhwc.push(testPointWeight)
    }

    an[NoSuchElementException] should be thrownBy lhwc.pop(insertions + 1)
  }

  test("should have the correct number of elements when inserting in different" +
    "threads") {
    val lhwc = new LocalHotWindowCollection()
    val threads = 100
    val insertionsPerThread = 100000
    val totalInsertions = threads * insertionsPerThread

    val threadRange = (0 until threads).par
    threadRange.foreach{ thread =>
      for (i <- 0 until insertionsPerThread) {
        lhwc.push(testPointWeight)
      }
    }
    lhwc.length should be (totalInsertions)
  }
}
