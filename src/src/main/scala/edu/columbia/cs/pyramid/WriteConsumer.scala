package edu.columbia.cs.pyramid

import java.io.PrintWriter
import java.util
import java.util.concurrent.{ArrayBlockingQueue, LinkedBlockingQueue}

class WriteConsumer(printWriter: PrintWriter,
                    queue: LinkedBlockingQueue[String],
                    bufferSize: Int = 10000)
  extends Runnable {

  private var finished = false

  // private val drainBuffer: util.ArrayList[String] = new util.ArrayList[String](bufferSize)
  private val drainBuffer: ArrayBlockingQueue[String] =
    new ArrayBlockingQueue[String](bufferSize)

  private def waitForQueue(totalWaitTime: Int): Unit = {
    if (queue.size() > 0 || finished) {
      return
    }
    val waitMS = 50
    val iterations = totalWaitTime / waitMS
    for (i <- (0 until iterations)) {
      if (!queue.isEmpty || finished) {
        return
      }
      Thread.sleep(waitMS)
    }
  }

  private def getNextPoint(): Option[String] = {
    drainBuffer.synchronized {
      //if (drainIterator == null || !drainIterator.hasNext) {
      if (drainBuffer.isEmpty) {
        waitForQueue(30000)
        drainBuffer.clear()
        if (queue.drainTo(drainBuffer, bufferSize) <= 0) {
          return None
        }
      }
      return Some(drainBuffer.poll)
    }
  }

  def run(): Unit = {
    var np: Option[String] = getNextPoint()
    while (!finished || !np.isEmpty) {
      if (!np.isEmpty) {
        np match {
          case Some(line) => printWriter.write(line)
          case None => throw new IllegalStateException(
            "Point was already matched as not being empty.")
        }
      }
      np = getNextPoint()
    }
    println("Finished writing.")
  }

  def end() = {
    finished = true
    println("Ending WriteConsumer: " + finished)
  }
}