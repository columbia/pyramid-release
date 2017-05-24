package edu.columbia.cs.pyramid


/**
  * Basically a queue but we leave the implementation specifics up to the
  * implementation. It can be a simple FIFO or it can use some other policy
  * for evicting items from the hot window. Expected to be thread safe.
  */
abstract class HotWindowCollection extends Serializable {

  /**
    * Returns the number of hot data points in the current window.
    *
    * @return
    */
  def length(): Int

  /**
    * Adds the point to the current hot window. Does not automatically pop
    * elements from the window.
    *
    * @param point
    * @return The number of elements in the queue.
    */
  def push(point: (LabeledPoint, Double)): Int

  /**
    *
    * @param count The number of elements to pop.
    * @return
    */
  def pop(count: Int): Seq[LabeledPoint]
  def pop(count: Long): Seq[LabeledPoint]

  /**
    * Returns a threadsafe iterator over the hot window.
    * @return
    */
  def getHotWindowIterator: Iterator[LabeledPoint]


}
