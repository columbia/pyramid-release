package edu.columbia.cs.pyramid

import java.io._
import java.util.concurrent.{LinkedBlockingQueue, LinkedBlockingDeque}

import com.fasterxml.jackson.core.JsonParser
import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.twitter.chill.{Input, Output, ScalaKryoInstantiator}

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.collection.parallel.ParSeq
import scala.io.{BufferedSource, Source}
import scala.util.Random



object BuildCountTable {

  def serializeFeaturizer(featurizer: Featurizer, outPath: String): Unit = {
    val countFileOutput = new Output(new FileOutputStream(outPath))
    val kryoInstantiator = new ScalaKryoInstantiator
    kryoInstantiator.setRegistrationRequired(false)
    val kryo = kryoInstantiator.newKryo()
    featurizer.cleanup()
    kryo.writeObject(countFileOutput, featurizer)
    countFileOutput.close()
  }

  def deserializeFeaturizer(inPath: String):
  Featurizer = {

    val kryoInstantiator = new ScalaKryoInstantiator
    kryoInstantiator.setRegistrationRequired(false)
    val kryo = kryoInstantiator.newKryo()
    val input = new Input(new FileInputStream(inPath))
    val newFeaturizer = kryo.readObject(input, classOf[Featurizer])
    input.close()
    return newFeaturizer
  }


  val gzRegex = "\\.gz\\.*$".r


  def buildCountTable(featurizer: PointFeaturization,
                      percentileFile: String,
                      dataFile: String,
                      featureCount: Int,
                      labels: Seq[Double],
                      countTableIsThreadSafe: Boolean = false,
                      bufferSize: Int = 100000) = {
    println("Intializing percentiles.")
    val percentiles = PyramidParser.readPercentileFile(percentileFile)
    var index = 0
    println("Counting input file.")

    val inputStream = CountTableLib.getInputStream(dataFile)

    val vwIterator = new VWIterator(inputStream, bufferSize, bufferSize,
      percentiles, featureCount)

    vwIterator.foreach{
      case (label: Double, weight: Option[Double], features: Array[Double], fs: String) =>
        if (index % 100000 == 0) {
          println("Counting line: " + index)
        }
        index += 1
        val point = new LabeledPoint(label, features)
        weight match {
          case Some(w) => featurizer.addObservation(point, w)
          case None => featurizer.addObservation(point)
        }
    }
  }


  def parseLabels(labels: Seq[String]): Seq[Double] = {
    return labels.map(l => l.toDouble)
  }

  def executeBuildCommand(wf: WindowedFeaturizer,
                           percentileFile: String,
                           countDataFile: String,
                           featureCount: Int,
                           labels: Seq[Double],
                           outFile: String): Unit = {
    buildCountTable(wf, percentileFile, countDataFile, featureCount, labels,
      false)
    CountTableLib.serializeWindowedFeaturizer(wf, outFile)
  }

  def executeBuildCommand(featurizer: Featurizer,
                           percentileFile: String,
                           countDataFile: String,
                           featureCount: Int,
                           labels: Seq[Double],
                           outFile: String): Unit = {
    buildCountTable(featurizer, percentileFile, countDataFile, featureCount,
      labels, false)
    serializeFeaturizer(featurizer, outFile)
  }

  def formatCriteoPoint(label: String, data: Array[String]): (String, Array[String]) = {
    val cleanData = data.map {
      case "" => "0.0"
      case x => x
    }
    if (label == "1") (label, cleanData) else ("-1", cleanData)
  }

  def updateNumericFeatures(data: Array[String],
                            numericMap: mutable.HashMap[Int, mutable.HashMap[Double, Int]]):
  mutable.HashMap[Int, mutable.HashMap[Double, Int]] = {
    synchronized {
      numericMap.keys.foreach { key =>
        try {
          val x_i = data(key).toDouble
          numericMap.get(key) match {
            case Some(mapValuesToCounts) =>
              mapValuesToCounts(x_i) = mapValuesToCounts.getOrElse(x_i, 0) + 1
            case None => throw new IllegalAccessException("this should never happen")
          }
        } catch {
          case _: java.lang.NumberFormatException => numericMap.remove(key)
        }
      }

      return numericMap
    }
  }

  def computePercentiles(countsMap: mutable.HashMap[Double, Int], numberOfPercentiles: Int): Array[Double] = {
    val tot = countsMap.values.sum
    var boundaries: Array[Int] = (1 until numberOfPercentiles).map { n =>
      math.round(n * (tot / numberOfPercentiles))
    }.toArray
    val percentiles: mutable.ArrayBuffer[Double] = mutable.ArrayBuffer[Double]()
    var counter = 0
    countsMap.keys.toArray.sorted.foreach { key =>
      counter += countsMap.getOrElse(key, 0)
      while (!boundaries.isEmpty && counter > boundaries.head) {
        percentiles += key
        boundaries = boundaries.drop(1)
      }
    }

    percentiles.toArray
  }

  def executePrepareCommand(args: Array[String]): Unit = {
    if (args.length != 7) {
      println("Prepare command requires <dataFile> <featureNumber> <saveFormattedData?> <formattedDataPath> <percentiles?> <percentilesPath> <numberOfPercentiles>")
      sys.exit(-1)
    }

    val dataFile: String = args(0)
    val featureNumber: Int = args(1).toInt
    val saveFormattedData: Boolean = args(2).toBoolean
    val formattedDataPath: String = args(3)
    val shouldComputePercentiles: Boolean = args(4).toBoolean
    val percentilesPath: String = args(5)
    val numberOfPercentiles: Int = args(6).toInt

    println("Starting to prepare.")

    val numericFeatureCounts: mutable.HashMap[Int, mutable.HashMap[Double, Int]] =
      mutable.HashMap[Int, mutable.HashMap[Double, Int]]()
    (0 until featureNumber).foreach { i =>
      numericFeatureCounts(i) = mutable.HashMap[Double, Int]()
    }
    val outWriter = if (saveFormattedData) CountTableLib.getOutputStream(formattedDataPath) else null
    val bufferSize = 100000
    var index = 0
    val rand = new Random
    CountTableLib.getInputStream(dataFile).getLines().grouped(bufferSize).foreach { lines =>
      if (index % 1000000 == 0) {
        println("Counting line: " + index)
      }

      val formattedPoints = lines.par.map { line =>
        val _data = line.split("\t")
        val label = _data.head
        val data: Array[String] = if (_data.tail.length < featureNumber) {
          _data.tail ++ Array.fill[String](_data.tail.length - featureNumber) {
            "0.0"
          }
        } else {
          _data.tail
        }

        val formattedPoint = formatCriteoPoint(label, data)
        // keep only some of the data to compute percentiles
        if (rand.nextFloat() < 0.01 && shouldComputePercentiles) {
          updateNumericFeatures(formattedPoint._2, numericFeatureCounts)
        }

        formattedPoint._1 + " |f " + formattedPoint._2.mkString(" ") + "\n"
      }
      index += bufferSize

      if (saveFormattedData) {
        formattedPoints.toList.foreach { point => outWriter.write(point) }
      }
    }
    outWriter.close()

    // now we can compute the percentiles
    if (shouldComputePercentiles) {
      val percentilesOutWriter = CountTableLib.getOutputStream(percentilesPath)
      numericFeatureCounts.foreach {
        case (featureIndex, countsMap) =>
          val percentiles: Array[Double] = computePercentiles(countsMap, numberOfPercentiles)
          percentilesOutWriter.write(
            featureIndex.toString + " | " + percentiles.mkString(" ") + "\n"
          )
      }
      percentilesOutWriter.close()
    }
  }


  def executePrepareMovielensCommand(args: Array[String]): Unit = {
    if (args.length != 16) {
      println("Prepare movielens command requires <ratingsFile> <moviesFile> " +
        "<formattedCountDataPath> <formattedCountTrainDataPath> <formattedCountTestDataPath> " +
        "<allMtrDataPath> <allMtrTrainDataPath> <allMtrTestDataPath> " +
        "<allRankDataPath> <allRankTrainDataPath> <allRankTestDataPath> " +
        "<countProportion> <trainProportion> <randomSampling?> <labelType>")
      sys.exit(-1)
    }

    val ratingsFile: String = args(0)
    val moviesFile: String = args(1)

    val formattedCountDataPath: String = args(2)
    val formattedCountTrainDataPath: String = args(3)
    val formattedCountTestDataPath: String = args(4)
    val formattedCountAllDataPath: String = args(5)

    val mtrAllDataPath: String = args(6)
    val mtrTrainDataPath: String = args(7)
    val mtrTestDataPath: String = args(8)

    val rankAllDataPath: String = args(9)
    val rankTrainDataPath: String = args(10)
    val rankTestDataPath: String = args(11)

    val countProportion: Double = args(12).toDouble
    val trainProportion: Double = args(13).toDouble

    val randomSampling: Boolean = args(14).toBoolean
    val labelType: String = args(15)

    val bufferSize = 100000

    val _allGenres: mutable.Set[String] = mutable.Set[String]()
    val movieMap: mutable.HashMap[String, Set[String]] = mutable.HashMap[String, Set[String]]()
    val movieRegex = "^([^,]*),\"?(.*)\"?,([^,]*)$".r

    val random = new Random

    println("Loading movies map.")

    CountTableLib.getInputStream(moviesFile).getLines().drop(1).grouped(bufferSize).foreach { lines =>
      lines
        .par
        .map { line =>
          val movieRegex(movieId, movieTitle, genres) = line
          if (genres == "(no genres listed)") {
            (movieId, Array[String]())
          } else {
            (movieId, genres.split('|'))
          }
        }
        .toArray
        .foreach { case (movieId: String, genres: Array[String]) =>
          genres.foreach { genre => _allGenres.add(genre) }
          movieMap(movieId) = genres.foldLeft(mutable.Set[String]()) { (set: mutable.Set[String], genre: String) =>
            set.add(genre)
            set
          }.toSet
        }
    }

    val allGenres: Array[String] = _allGenres.toArray.sorted

    println("Starting movielens prepare.")

    var totalN = 0
    var timestamps = Array[Long]()
    if (!randomSampling) {
      CountTableLib.getInputStream(ratingsFile).getLines().drop(1).grouped(bufferSize).foreach { lines =>
        totalN += bufferSize
        val formattedPoints = lines.par.map { line =>
          val data: Array[String] = line.split(",")
          val timestamp: Long = data(3).toLong
          timestamp
        }.toArray
        timestamps = timestamps ++ formattedPoints
      }
      timestamps = timestamps.sorted
    }

    val countTimestampThreshold = if (!randomSampling) timestamps((totalN * countProportion).toInt) else 0
    val trainTimestampThreshold = if (!randomSampling) timestamps(((trainProportion + countProportion) * totalN).toInt) else 0

    var index = 0
    val countDataWriter = CountTableLib.getOutputStream(formattedCountDataPath)
    val trainCountDataWriter = CountTableLib.getOutputStream(formattedCountTrainDataPath)
    val testCountDataWriter = CountTableLib.getOutputStream(formattedCountTestDataPath)
    val allCountDataWriter = CountTableLib.getOutputStream(formattedCountAllDataPath)
    val mtrAllDataWriter = CountTableLib.getOutputStream(mtrAllDataPath)
    val mtrTrainDataWriter = CountTableLib.getOutputStream(mtrTrainDataPath)
    val mtrTestDataWriter = CountTableLib.getOutputStream(mtrTestDataPath)
    val rankAllDataWriter = CountTableLib.getOutputStream(rankAllDataPath)
    val rankTrainDataWriter = CountTableLib.getOutputStream(rankTrainDataPath)
    val rankTestDataWriter = CountTableLib.getOutputStream(rankTestDataPath)

    CountTableLib.getInputStream(ratingsFile).getLines().drop(1).grouped(bufferSize).foreach { lines =>
      if (index % 100000 == 0) {
        println("Counting line: " + index)
      }

      val formattedPoints = lines.par.map { line =>
        val data: Array[String] = line.split(",")
        val userId: String = data(0)
        val movieId: String = data(1)
        val rating: String = data(2)
        val timestamp: Long = data(3).toLong
        val movieGenres: Set[String] = movieMap.getOrElse(movieId, Set[String]())

        // val label: String = if(integerLabels) (rating.toDouble * 2).toInt.toString else rating
        val label: String = labelType match {
          case "integer"  => (rating.toDouble * 2).toInt.toString
          case "binary"   => if (rating.toDouble >= 4.0) "1" else "-1"
          case _          => rating
        }

        val dataLine = label +
        " |f " + "user-" + userId + " " + "movie-" + movieId + " " +
          allGenres.map { genre =>
            val movieOfGenre = movieGenres.contains(genre)
            if (movieOfGenre) genre else "no-" + genre
          }.mkString(" ") + "\n"

        val mtrLine = label +
          " |g " + "movie-"+movieId + " " + "user-"+userId + " " + movieGenres.mkString(" ") +
          " |u " + userId+"-movie-"+movieId + " " + movieGenres.map { genre => userId + "-" + genre }.mkString(" ") +
          "\n"

        val rankLine = label +
          " |m " + "movie-"+movieId +
          " |u " + "user-"+userId +
          " |g " + movieGenres.mkString(" ") +
          "\n"

        (dataLine, mtrLine, rankLine, timestamp)
      }.toArray

      formattedPoints.foreach { case (dataLine, mtrLine, rankLine, timestamp) =>
        val dataset: String = (randomSampling, timestamp, random.nextFloat) match {
          case (false, x, _) if x < countTimestampThreshold => "counting"
          case (false, x, _) if x >= countTimestampThreshold && x < trainTimestampThreshold => "training"
          case (false, x, _) if x >= trainTimestampThreshold => "testing"
          case (true, _, x) if x < countProportion => "counting"
          case (true, _, x) if x >= countProportion && x < trainProportion + countProportion => "training"
          case (true, _, x) if x >= countProportion + trainProportion => "testing"
        }

        dataset match {
          case "counting" =>
            // data for count table
            countDataWriter.write(dataLine)
            allCountDataWriter.write(dataLine)
            mtrAllDataWriter.write(mtrLine)
            rankAllDataWriter.write(rankLine)
          case "training" =>
            // data for training
            trainCountDataWriter.write(dataLine)
            allCountDataWriter.write(dataLine)
            mtrAllDataWriter.write(mtrLine)
            mtrTrainDataWriter.write(mtrLine)
            rankAllDataWriter.write(rankLine)
            rankTrainDataWriter.write(rankLine)
          case "testing" =>
            // data for testing
            testCountDataWriter.write(dataLine)
            mtrTestDataWriter.write(mtrLine)
            rankTestDataWriter.write(rankLine)
        }
      }
      index += bufferSize
    }
    countDataWriter.close()
    trainCountDataWriter.close()
    testCountDataWriter.close()
    mtrAllDataWriter.close()
    mtrTrainDataWriter.close()
    mtrTestDataWriter.close()
    rankAllDataWriter.close()
    rankTrainDataWriter.close()
    rankTestDataWriter.close()
  }

  def parseConfigFile(configPath: String, args: Array[String]): Unit = {
    val source = scala.io.Source.fromFile(configPath)
    try {
      val objMapper = new ObjectMapper()
      objMapper.configure(JsonParser.Feature.ALLOW_COMMENTS, true)
      objMapper.configure(JsonParser.Feature.ALLOW_SINGLE_QUOTES, true)
      objMapper.registerModule(DefaultScalaModule)
      val configString = source.getLines().mkString("\n")
      val configTree = objMapper.readTree(configString)

      require(configTree.has("command"))
      val command = configTree.get("command").asText()
      println("Command " + command)

      val operations: Seq[PyramidOperation] = Seq[PyramidOperation](
        new TransformOperation,
        new BuildOperation,
        new DiscretizeOperation,
        new ComputeNoiseWeightsOperation,
        new PrepareCriteoKaggleOperation
      )
      val operationsWithName = operations.filter(_.operationName == command)
      require(operationsWithName.length == 1)
      println(operationsWithName.head.operationName)
      operationsWithName.head.execute(configPath)

    } finally {
      source.close()
    }
  }

  def main(args: Array[String]) = {
    if (args.length == 0) {
      println("Exepcted <config file>")
      sys.exit(-1)
    }
    parseConfigFile(args(0), args.tail)
  }
}
