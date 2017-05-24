package edu.columbia.cs.pyramid

import scala.collection.mutable.Queue

/**
  * Currently a very naive implementation of a thradsafe seq. Just serializes
  * accesses to an ArrayBuffer.
  */
class LocalHotWindowCollection extends HotWindowCollection {

  private val hotWindow = new Queue[(LabeledPoint, Double)]()

  /**
    * Returns the number of hot data points in the current window.
    *
    * @return
    */
  override def length(): Int = {
    this.synchronized {
      return hotWindow.length
    }
  }

  /**
    * Returns a threadsafe iterator over the hot window.
    *
    * @return
    */
  override def getHotWindowIterator: Iterator[LabeledPoint] = {
    this.synchronized {
      return hotWindow.clone().map {
        case (point: LabeledPoint, weight: Double) =>
          point
      }.iterator
    }
  }

  /**
    * Adds the point to the current hot window. Does not automatically pop
    * elements from the window.
    *
    * @param point
    * @return The number of elements in the queue.
    */
  override def push(point: (LabeledPoint, Double)): Int = {
    this.synchronized {
      hotWindow.enqueue(point)
      return hotWindow.length
    }
  }

  /**
    *
    * @param count The number of elements to pop.
    * @return
    */
  override def pop(count: Int): Seq[LabeledPoint] = {
    return pop(count.toLong)
  }

  override def pop(count: Long): Seq[LabeledPoint] = {

    this.synchronized {
      return (0L until count).map(i => hotWindow.dequeue()._1)
    }
  }
}