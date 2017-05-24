package edu.columbia.cs.pyramid


import java.util
import java.util.concurrent.LinkedBlockingQueue

import scala.io.BufferedSource


class ParserJob(private val inputSource: BufferedSource,
                private val pointQueue:
                LinkedBlockingQueue[(Double, Option[Double], Array[Double], String)],
                private val percentiles: Map[Int, Array[Double]],
                private val featureCount: Int,
                private val bufferSize: Int = 10000)
  extends Runnable {

  private var finished = false

  private var parsedLines = 0
  override def run(): Unit = {
    inputSource.getLines().grouped(bufferSize).foreach { lines =>
      println("ParsedLines: " + parsedLines)
      val processedLines: Seq[(Double, Option[Double], Array[Double], String)] =
        lines.par.map { line =>
          val featureString = PyramidParser.parseFeaturesFromFileLine(line)
          val x = PyramidParser.parseDataFileLine(line, percentiles, featureCount)
          (x._1, x._2, x._3, featureString)
        }.toList
      parsedLines += processedLines.length
      processedLines.foreach(point => pointQueue.put(point))
    }
    finished = true
  }

  def isFinished(): Boolean = finished
}

class VWIterator(private val inputSource: BufferedSource,
                 private val queueBufferSize: Int = 10000,
                 private val groupBufferSize: Int = 10000,
                 private val percentiles: Map[Int, Array[Double]],
                 private val featureCount: Int
                ) extends Iterator[(Double, Option[Double], Array[Double], String)] {

  private val pointQueue =
    new LinkedBlockingQueue[(Double, Option[Double], Array[Double], String)](queueBufferSize)
  private val parserJob = new ParserJob(inputSource, pointQueue, percentiles,
    featureCount, groupBufferSize)
  private val parserJobThread = new Thread(parserJob)
  parserJobThread.start()
  private var finished = false

  private val drainBuffer =
    new util.ArrayList[(Double, Option[Double], Array[Double], String)](queueBufferSize)

  private var drainIterator: util.ListIterator[
    (Double, Option[Double], Array[Double], String)]= null

  private var nextPoint: Option[ (Double, Option[Double], Array[Double], String)] = None

  private def waitForQueue(totalWaitTime: Int): Unit = {
    if (pointQueue.size() > 0 || parserJob.isFinished) {
      return
    }
    val waitMS = 50
    val iterations = totalWaitTime / waitMS
    for (i <- (0 until iterations)) {
      if (pointQueue.size() > 0 || parserJob.isFinished) {
        return
      }
      Thread.sleep(waitMS)
    }
  }

  private def getNextPoint(): Option[(Double, Option[Double], Array[Double], String)] = {
    if (drainIterator == null || !drainIterator.hasNext) {
      waitForQueue(30000)

      drainBuffer.clear()
      if (pointQueue.drainTo(drainBuffer, queueBufferSize) == 0) {
        finished = true
        return None
      }
      println("Drained points: " + drainBuffer.size)
      drainIterator = drainBuffer.listIterator()
    }
    return Some(drainIterator.next())
  }

  override def hasNext: Boolean = {
    if (finished) {
      return false
    }
    this.synchronized {
      return nextPoint match {
        case Some(np) => true
        case None => {
          getNextPoint() match {
            case Some(np) => {
              nextPoint = Some(np)
              return true
            }
            case None => false
          }
        }
      }
    }
  }

  override def next(): (Double, Option[Double], Array[Double], String) = {
    if (hasNext) {
      this.synchronized {
        return nextPoint match {
          case Some(np) => {
            nextPoint = None
            np
          }
          case None => throw new IllegalStateException(
            "Was reported to have next point but found no point.")
        }
      }
    } else {
      println("Found no more points.")
      finished = true
      return null
    }
  }
}