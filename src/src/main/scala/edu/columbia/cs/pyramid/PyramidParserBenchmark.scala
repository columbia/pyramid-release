package edu.columbia.cs.pyramid

import java.io.File
import java.io.PrintWriter

object PyramidParserBenchmark {

  val percentiles = Map[Int, Array[Double]]()

  val bitPrecision = 26

  def writeCSV(outPath: String,
               percentiles: Seq[Double],
               data: Seq[(Long, Seq[Long])]): Unit = {
    val fd = new File(outPath)
    fd.createNewFile()
    val pw = new PrintWriter(fd)
    val header = "," ++ percentiles.map(_.toString).mkString(",")
    pw.write(header)
    pw.write("\n")
    data.foreach{
      case(features: Long, values: Seq[Long]) =>
        pw.write(features.toString)
        pw.write(",")
        pw.write(values.map(_.toString).mkString(","))
        pw.write("\n")
    }
    pw.close()
  }

  def median(x: Seq[Long]): Long = {
    val s = x.sorted
    return s(x.length / 2)
  }

  def calculatePercentiles(x: Seq[Long], percentiles: Seq[Double]): Seq[Long] = {
    val s = x.sorted
    return percentiles.map{percentile =>
      val index = (x.length * percentile).toInt
      s(index)
    }
  }

  def generateFeatureVector(features: Int,
                            namespaces: Int,
                            keys: Boolean): String = {
    require(features > 0)
    require(namespaces > 0)
    val rand = new scala.util.Random()
    var sb = new StringBuilder().append(rand.nextInt(2)).append(" ")
    for (i <- 0 until namespaces) {
      sb = sb.append("|ns").append(i).append(" ")
      for (j <- 0 until features) {
        if (keys && rand.nextBoolean()) {
          sb = sb.append(rand.nextInt()).append(":")
        }
        sb = sb.append(rand.nextInt()).append(" ")
      }
    }

    return sb.toString()
  }

  def generateFeatureVectors(lines: Int,
                             features: Int,
                             namespaces: Int,
                             keys: Boolean): Seq[String] = {
    return (0 until lines).map(x =>
      generateFeatureVector(features, namespaces, keys))
  }

  def namespaceTime(line: String): Long = {
    val startTime = System.nanoTime()
    val (_, parsedData) = NamespacedParser.parseDataLine(line, percentiles)
    new FeatureSpace(parsedData, bitPrecision)
    val endTime = System.nanoTime()
    return endTime - startTime
  }

  def simpleTime(line: String, featureCount: Int): Long = {
    val startTime = System.nanoTime()
    PyramidParser.parseDataFileLine(line, percentiles, featureCount)
    val endTime = System.nanoTime()
    return endTime - startTime
  }

  def runBenchmark(lines: Seq[String],
                   features: Int,
                   namespaceParser: Boolean,
                   simpleParser: Boolean): (Seq[Long], Seq[Long]) = {
    require(namespaceParser || simpleParser)
    val namespaceTimes: Seq[Long] = namespaceParser match {
      case true => lines.map(line => namespaceTime(line))
      case false => Seq[Long]()
    }
    val simpleTimes: Seq[Long] = simpleParser match {
      case true => lines.map(line => simpleTime(line, features))
      case false => Seq[Long]()
    }
    return (namespaceTimes, simpleTimes)
  }

  def main(args: Array[String]): Unit = {
    val percentiles: Seq[Double] = Seq[Double](0.25, 0.5, 0.75, 0.90, 0.95, 0.99)
    val lineCount = 10000

    // Benchmark for both simple and namespace parser.
    val comparisonFeatureCounts = Seq[Int](10, 50, 100, 500, 1000)
    val testLines = comparisonFeatureCounts.map(featureCount =>
      (featureCount, generateFeatureVectors(lineCount, featureCount, 1, false)))
    val simpleData: Seq[(Long, Seq[Long])] = testLines.map{
      case (featureCount: Int, testLines: Seq[String]) =>
        val times = runBenchmark(testLines, featureCount, false, true)
        val timePercentiles = calculatePercentiles(times._2, percentiles)
        (featureCount.toLong, timePercentiles)
    }
    val namespaceData: Seq[(Long, Seq[Long])] = testLines.map{
      case (featureCount: Int, testLines: Seq[String]) =>
        val times = runBenchmark(testLines, featureCount, true, false)
        val timePercentiles = calculatePercentiles(times._1, percentiles)
        (featureCount.toLong, timePercentiles)
    }
    writeCSV("simpleTimes.csv", percentiles, simpleData)
    writeCSV("namespaceTimes.csv", percentiles, namespaceData)

  }
}
