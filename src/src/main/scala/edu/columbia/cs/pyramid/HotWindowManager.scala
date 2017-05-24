package edu.columbia.cs.pyramid

import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.locks.ReentrantReadWriteLock

import scala.collection.mutable.Queue
import scala.util.Random

/**
  *
  * @param hotWindowSize               The number of points to maintain in hot storage.
  * @param countTableCount             The number of count tables to maintain.
  * @param countTableSize              The number of points to include in each table.
  * @param masterNoise                 The amount of noise to include in the master count
  *                                    table(s).
  * @param hotWindowCollection         Data structure used to maintain the hot window.
  * @param instantiateMasterFeaturizer Function to instantiate the master featurizer.
  * @param instantiateWindowFeaturizer Function to instantiate the window featurizers.
  *
  */
class HotWindowManager(
                        val hotWindowSize: Int,
                        val countTableCount: Double,
                        val countTableSize: Int,
                        val masterNoise: Option[Double],
                        val hotWindowCollection: HotWindowCollection,
                        val instantiateWindowFeaturizer: (String) => Featurizer,
                        val instantiateMasterFeaturizer: (String) => Featurizer
                      ) extends PointFeaturization {

  // The number of elements added since the last window flush.
  private val hotWindowCount: AtomicLong = new AtomicLong(0)

  private val frozenFeaturizers = new Queue[Featurizer]()

  private var currentFeaturizer: Featurizer =
    instantiateWindowFeaturizer(hotWindowCount.get().toString)

  private var mergedFeaturizer: Option[Featurizer] = None

  // Lock to control when the featurizer is flushed. Must use the read lock
  // when adding points to the to the currentFeaturizer and acquire the write
  // lock when the currentFeaturizer.
  val featurizerFlushLock = new ReentrantReadWriteLock()

  private def acquireExclusiveLock(): Unit = {
    featurizerFlushLock.writeLock().lock()
  }

  private def releaseExclusiveLock(): Unit = {
    featurizerFlushLock.writeLock().unlock()
  }

  private def acquireConcurrentLock(): Unit = {
    featurizerFlushLock.readLock().lock()
  }

  private def releaseConcurrentLock(): Unit = {
    featurizerFlushLock.readLock().unlock()
  }

  /**
    * Internal function to add the observation and return the current hot window
    * size and cycle count.
    *
    * @param point
    * @param by
    * @return
    */
  private def internalAddObservation(point: LabeledPoint, by: Double = 1.0):
  (Long, Long) = {
    acquireConcurrentLock()
    try {
      val currentHotWindowSize = hotWindowCollection.push((point, by))
      val currentCycleCount = hotWindowCount.incrementAndGet()
      currentFeaturizer.addObservation(point, by)
      return (currentHotWindowSize, currentCycleCount)
    } finally {
      releaseConcurrentLock()
    }

  }

  /**
    * If the currentCycleCount % the hot window size is zero then it will
    * 1. Remove hotWindowSize - hotWindowCollectionSize.length elements from the
    * hot window collection.
    * 2. Remove the oldest featurizer from the frozenFeaturizers.
    * 3. Add the current featurizer to the frozenFeaturizers.
    * 4. Create a new featurizer.
    * 5. Dirty the merged featurizer.
    *
    * @param currentHotWindowSize
    * @param currentCycleCount
    */
  private def flushFeaturizerIfNeeded(currentHotWindowSize: Long,
                                      currentCycleCount: Long) = {
    if (currentCycleCount > 0 && (currentCycleCount % hotWindowSize == 0 ||
      currentCycleCount % countTableSize == 0)) {
      acquireExclusiveLock()
      try {
        // Should we pop elements from the hot window?
        if (currentCycleCount % hotWindowSize == 0) {
          val hotWindowPopCount = hotWindowCollection.length() - hotWindowSize
          if (hotWindowPopCount > 0) {
            hotWindowCollection.pop(hotWindowPopCount)
          }
        }
        // Should we pop the current featurizer?
        if (currentCycleCount % countTableSize == 0) {
          if (frozenFeaturizers.length == countTableCount) {
            frozenFeaturizers.dequeue()
          }
          frozenFeaturizers += currentFeaturizer
          currentFeaturizer = instantiateWindowFeaturizer(currentCycleCount.toString)
          mergedFeaturizer = None
        }
      } finally {
        releaseExclusiveLock()
      }
    }
  }

  /**
    * Adds a point to the hot window and bumps the current window if needed.
    *
    * @param point
    * @param by
    */
  override def addObservation(point: LabeledPoint, by: Double = 1.0): Unit = {
    val (currentHotWindowSize, currentCycleCount) =
      internalAddObservation(point, by)
    flushFeaturizerIfNeeded(currentHotWindowSize, currentCycleCount)
  }

  private def mergeFeaturizers(mergeCurrentHotWindow: Boolean = false):
  Featurizer = {
    acquireExclusiveLock()
    try {
      val mf = instantiateMasterFeaturizer(hotWindowCount.get().toString)
      if (mergeCurrentHotWindow) {
        println("Merging current featurizer.")
        mf.merge(currentFeaturizer)
      }
      var i = 0
      for (q <- frozenFeaturizers) {
        println("Merging featurizer: " + i + " / " + frozenFeaturizers.length)
        i += 1
        mf.merge(q)
      }
      mergedFeaturizer = Some(mf)
      return mf
    } finally {
      releaseExclusiveLock()
    }
  }

  def getMergedWindow(mergeCurrentHotWindow: Boolean = false): Featurizer =
    mergedFeaturizer match {
      case Some(mf) => mf
      case None => mergeFeaturizers(mergeCurrentHotWindow)
    }

  /**
    * Featurizes the current hot window and returns those featurized points.
    *
    * @return
    */
  def featurizeHotWindow(): Seq[LabeledPoint] = {
    val mf = getMergedWindow()
    return hotWindowCollection.getHotWindowIterator.map { point =>
      val fv = mf.featurizePoint(point)
      new LabeledPoint(point.label, fv)
    }.toSeq
  }

  override def featurizePoint(point: LabeledPoint,
                              removeFirstLabel: Boolean = false,
                              featurizeWithCounts: Boolean,
                              featurizeWithTotalCount: Boolean = false,
                              featurizeWithProbabilities: Boolean,
                              addBackingNoise: Boolean,
                              defaultProbaCriteria: DefaultProbaCriteriaConfig = DefaultProbaCriteriaConfig(),
                              window: Option[Int] = None): Array[Double] = {
    require(window.isEmpty)
    return getMergedWindow(true).featurizePoint(point, removeFirstLabel, featurizeWithCounts,
      featurizeWithTotalCount, featurizeWithProbabilities, addBackingNoise, defaultProbaCriteria)
  }

  override def getLabelProbability(label: Double,
                                   labelCounts: Int,
                                   featureCount: Double,
                                   fvTotal: Double,
                                   window: Option[Int] = None): Double = {
    require(window.isEmpty)
    return getMergedWindow().getLabelProbability(label, labelCounts,
      featureCount, fvTotal, None)
  }

  override def countForFeature(label: Double,
                               featureVector: Array[Double],
                               featureIndices: Array[Int],
                               addBackingNoise: Boolean,
                               window: Option[Int] = None): Double = {
    require(window.isEmpty)
    return getMergedWindow().countForFeature(label, featureVector,
      featureIndices, addBackingNoise)
  }

  override def featurizePoints(points: Seq[LabeledPoint],
                               window: Option[Int] = None):
  Seq[Array[Double]] = {
    return getMergedWindow().featurizePoints(points)
  }

  override def cleanup(): Unit = {
    currentFeaturizer.cleanup()
    mergedFeaturizer match {
      case Some(mf) => mf.cleanup()
      case None => None
    }
    frozenFeaturizers.foreach(ff => ff.cleanup())
  }
}
