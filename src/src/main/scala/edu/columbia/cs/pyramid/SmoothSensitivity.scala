package edu.columbia.cs.pyramid

import scala.reflect.ClassTag
import scala.util.Random

object SmoothSensitivity {

  case class JListElement(b: Int, j: Int, sensitivity: Double)

  /**
    * Returns a random variable from a laplace distribution.
    *
    * Based on: https://www.johndcook.com/SimpleRNG.cpp
    *
    * @param mean
    * @param scale
    * @return
    */
  def laplace(mean: Double, scale: Double): Double = {
    require(scale >= 0)
    if (scale == 0) {
      return 0.0
    }
    val p = Random.nextFloat()
    if (p < 0.5) {
      return mean + scale * math.log(2.0 * p)
    } else {
      return mean - scale * math.log(2.0 * (1 - p))
    }
  }

  /**
    * Returns a variable from a cauchy distribtion.
    *
    * Based on: https://www.johndcook.com/SimpleRNG.cpp
    *
    * @param median
    * @param scale
    * @return
    */
  def cauchy(median: Double, scale: Double): Double = {
    return median + scale * math.tan(math.Pi * (Random.nextFloat() - 0.5))
  }

  /**
    * Returns the indexes of an array to sample.
    *
    * @param indexes
    * @param samples
    * @param withReplacement
    * @return
    */
  def getIndexesToSample(indexes: Int,
                         samples: Int,
                         withReplacement: Boolean): Seq[Int] = {
    if (withReplacement) {
      return (0 until samples).map { i =>
        Random.nextInt(indexes)
      }
    } else {
      require(samples <= indexes)
      return Random.shuffle((0 until indexes).toList).take(samples)
    }
  }

  def countAppearances[T: ClassTag](dataset: Seq[T]): Map[T, Int] = {
    val counts: scala.collection.mutable.HashMap[T, Int] =
      new scala.collection.mutable.HashMap[T, Int]()
    dataset.foreach { t =>
      counts(t) = counts.get(t) match {
        case Some(i) => i + 1
        case None => 1
      }
    }

    return counts.toMap
  }

  /**
    * Returns a
    *
    * @param dataset
    * @param m
    * @param observationsPerSample
    * @tparam T
    * @return
    */
  def sample[T: ClassTag](dataset: Array[T],
                          m: Int,
                          observationsPerSample: Int): Array[Array[T]] = {
    val indexesToSample: Seq[Seq[Int]] = (0 to m).map { i: Int =>
      getIndexesToSample(dataset.length, observationsPerSample, false)
    }
    val maxObservations: Int = countAppearances(indexesToSample.flatten).map(_._2).max
    if (maxObservations > math.sqrt(m)) {
      return sample(dataset, m, observationsPerSample)
    } else {
      return indexesToSample.map { indexes =>
        indexes.map { index =>
          dataset(index)
        }.toArray
      }.toArray
    }
  }

  /**
    * Partitions the dataset into n partitions.
    *
    * @param dataset
    * @param m
    * @tparam T
    * @return
    */
  def partition[T: ClassTag](dataset: Array[T],
                             m: Int): Array[Array[T]] = {
    val partitionedData: Seq[Seq[T]] = Random.shuffle((0 until dataset.length).toList)
      .zipWithIndex.map {
      case (shuffledIndex: Int, currentIndex: Int) =>
        (currentIndex % m, dataset(shuffledIndex))
    }.groupBy(_._1).map {
      case (i: Int, v: Seq[(Int, T)]) =>
        v.map(_._2)
    }.toSeq
    partitionedData.map { s =>
      s.toArray
    }.toArray
  }

  /**
    * Returns the median of the dataset and throws and requires that the length
    * of the input array be greater than 0.
    *
    * @param dataset
    * @return
    */
  def getMedian(dataset: Array[Double]): Double = {
    require(dataset.length > 0)
    if (dataset.length == 1) {
      return dataset(0)
    }
    val sortedDataset = dataset.sorted
    if (dataset.length % 2 == 1) {
      return sortedDataset(dataset.length / 2)
    } else {
      return (sortedDataset(sortedDataset.length / 2 - 1)
        + sortedDataset(sortedDataset.length / 2)) / 2
    }
  }

  /**
    * Returns the median of a set of single element arrays.
    *
    * @param dataset
    * @return
    */
  def getMedian(dataset: Array[Array[Double]]): Array[Double] = {
    val elementDataset = dataset.map { d =>
      require(d.length == 1)
      d(0)
    }
    return Array(getMedian(elementDataset))
  }

  /**
    * Returns the epsilon smooth upper bound of the median.
    *
    * TODO: The smoooth sensitivity paper has notes about a more efficient approach but we do not implement it here.
    *
    * @param dataset           The dataset over which the bound should be taken.
    * @param globalSensitivity The number of record changes to protect.
    * @param beta              Beta-smooth upper-bound of local sensitivity.
    * @return
    */
  def medianSmoothSensitivity(dataset: Array[Array[Double]],
                              globalSensitivity: Double,
                              beta: Double): (Array[Double], Double) = {
    return SmoothSensitivity.quantileSmoothSensitivity(
      dataset, 0.5, globalSensitivity, 0, beta
    )
  }

  /**
    * Returns the epsilon smooth upper bound of the median.
    *
    * TODO: The smoooth sensitivity paper has notes about a more efficient approach but we do not implement it here.
    *
    * @param dataset  The dataset over which the bound should be taken.
    * @param quantile The quantile to compute, in (0,1).
    * @param maxValue The max val of dataset's range.
    * @param minValue The min val of dataset's range.
    * @param beta     Compute a beta-smooth upper-bound of local sensitivity.
    * @return
    */
  def quantileSmoothSensitivity(dataset: Array[Array[Double]],
                                quantile: Double,
                                maxValue: Double,
                                minValue: Double,
                                beta: Double): (Array[Double], Double) = {
    require(dataset.length > 2)
    require(0 <= quantile && quantile <= 1)

    val sortedDataset: Array[Double] = dataset.map { a =>
      require(a.length == 1)
      a(0)
    }.sorted

    val quantileIndex: Int = math.floor(dataset.length * quantile).toInt
    val quantileValue = sortedDataset(quantileIndex)

    val smoothSensitivity = (0 to sortedDataset.length).map { k =>

      val e_k_beta = Math.pow(Math.E, -1 * k * beta)

      val k_local_sensitivity = (0 to k + 1).map { t =>
        val index1 = quantileIndex + t
        val val1 = if (index1 >= dataset.length) maxValue else sortedDataset(index1)

        val index2 = quantileIndex + t - k - 1
        val val2 = if (index2 < 0) minValue else sortedDataset(index2)

        val1 - val2
      }.max
      // println(k, k_local_sensitivity, e_k_beta, e_k_beta * k_local_sensitivity)
      e_k_beta * k_local_sensitivity
    }.max
    return (Array(quantileValue), smoothSensitivity)
  }

  def quantileFastSmoothSensitivity(dataset: Array[Array[Double]],
                                    quantile: Double,
                                    maxValue: Double,
                                    minValue: Double,
                                    beta: Double): (Array[Double], Double) = {
    require(dataset.length > 2)
    require(0 <= quantile && quantile <= 1)
    require(maxValue > minValue)

    val sortedExtendedDataset: Array[Double] = (dataset.map { a =>
      require(a.length == 1)
      a(0)
    } ++ Array[Double](minValue, maxValue)).sorted

    // NOTE: the math.floor is a convention here
    val n = sortedExtendedDataset.length
    val quantileIndex = math.floor((n-2) * quantile + 1).toInt
    val quantileValue = sortedExtendedDataset(quantileIndex)
    val jList = _jList(sortedExtendedDataset, beta, 0, quantileIndex, quantileIndex, n-1)
    // println(jList)
    val smoothSensitivity = jList.map { x => x.sensitivity }.max
    return (Array[Double](quantileValue), smoothSensitivity)
  }


  //NOTE: dataset has to be sorted
  def _jList(dataset: IndexedSeq[Double],
             beta: Double,
             a: Int,
             c: Int,
             L: Int,
             U: Int): Seq[JListElement] = {

    if (c < a) {
      // println(f"DEBUG: breaking => $c < $a")
      return Seq[JListElement]()
    }
    require(L <= U)

    val b = math.floor((a + c) / 2).toInt

    val xb = dataset(b)
    // println(f"DEBUG: a:$a, c:$c, L:$L, U:$U, b:$b")
    val jbArgMax: (Double, Int) = (L to U).map { j =>
      val xj = dataset(j)
      val ljb = (xj - xb) * math.pow(math.E, beta * (b - j + 1))
      (ljb, j)
    }.sorted.reverse.head

    val jb: Int = jbArgMax._2

    return _jList(dataset, beta, a, b - 1, L, jb) ++ Seq[JListElement](JListElement(b, jb, jbArgMax._1)) ++ _jList(dataset, beta, b + 1, c, jb, U)
  }

  /**
    * Returns the t-th closest point in Z to c.
    *
    * @param c                 The point for which the tRadius should be computed.
    * @param Z                 The set of points over which distances are computed.
    * @param t                 Uses base 0.
    * @param globalSensitivity Returned when t > size of Z.
    * @param distance          The distance function.
    * @return
    */
  def getTRadius(c: Array[Double],
                 Z: Array[Array[Double]],
                 t: Int,
                 globalSensitivity: Double,
                 distance: (Array[Double], Array[Double]) => Double): Double = {
    if (t >= Z.length) {
      return globalSensitivity
    }
    val distances = Z.map { z =>
      distance(c, z)
    }.sorted
    return distances(t)
  }

  def getTRadius(index: Int,
                 t: Int,
                 globalSensitivity: Double,
                 distances: Array[(Double, Int, Int)]): Double = {
    val iDistances = distances.filter {
      case (distance: Double, i: Int, j: Int) =>
        index == i
    }
    if (iDistances.length < t) {
      return globalSensitivity
    }
    return iDistances(t)._1
  }

  def euclideanDistance(p1: Array[Double],
                        p2: Array[Double]): Double = {
    require(p1.length == p2.length)
    val s = p1.zip(p2).map {
      case (x1: Double, x2: Double) =>
        math.pow(x1 - x2, 2)
    }.sum
    return math.sqrt(s)
  }

  /**
    * Computes and returns the pairwise distances between all points in the
    * dataset.
    *
    * @param z
    * @param distance
    * @return
    */
  def getPairWiseDistances(z: Array[Array[Double]],
                           distance: (Array[Double], Array[Double]) => Double):
  Array[(Double, Int, Int)] = {

    return (0 until z.length).map { i =>
      (0 until z.length).map { j =>
        val d = if (i == j) {
          -1
        } else {
          distance(z(i), z(j))
        }
        (d, i, j)
      }
    }.flatten.toArray.filter {
      case (d: Double, i: Int, j: Int) =>
        d >= 0
    }
  }

  /**
    * Returns the center of attention and the smooth sensitivity of the CoA.
    *
    * @param dataset
    * @param globalSensitivity
    * @param beta Beta-smooth upper-bound of local sensitivity.
    * @return
    */
  def centerOfAttentionSmoothSensitivity(dataset: Array[Array[Double]],
                                         globalSensitivity: Double,
                                         beta: Double): (Array[Double], Double) = {


    val m: Int = dataset.length
    val s: Double = math.sqrt(m)
    val a: Double = s / beta
    // Rounding up seems like an appropriately conservative approach.
    val t0: Int = math.ceil((m + s) / 2 + 1).toInt

    val sortedPairwiseDistances = getPairWiseDistances(dataset,
      euclideanDistance).sorted

    val tRaddii: IndexedSeq[IndexedSeq[(Double, Int)]] = (0 until m).map { t =>
      (0 until m).map { index =>
        val distance = getTRadius(index, t, globalSensitivity,
          sortedPairwiseDistances)
        (distance, index)
      }.sorted
    }
    val minT0: (Double, Int) = tRaddii(t0)(0)

    // TODO: Calculate sensitivity.

    throw new NotImplementedError("Sensitivity is not implemented for CoA")


    return (dataset(minT0._2), -1.0)
  }

  /**
    * Performs a sample and aggregate.
    *
    * TODO: Is it possible to get epsilon DP w/ Laplace distribution and smooth
    * sensitivity? The paper makes it seem like the Cauchy distribution is
    * required for that.
    *
    * @param dataset           The dataset used to compute the function. It is
    *                          expected to be an array of vectors.
    * @param m                 The number of samples to take.
    * @param globalSensitivity The global sensitivity of the base function.
    * @param epsilon           The privacy parameter
    * @param delta             The "failure" parameter for approximate differential privacy.
    * @param application       The function to apply to each sample. Expects an
    *                          array of vectors and returns a vector.
    * @param aggregation       Function with a known smooth sensitivity that returns the sensitivity.
    *                          Takes three parameters:
    *                          1) The vectors to be aggregated.
    *                          2) The global sensitivity of the aggregation function.
    *                          3) Epsilon, the privacy parameter.
    *                          4) Beta value of the smooth sampling.
    * @return
    */
  def sampleAggregate(dataset: Array[Array[Double]],
                      m: Int,
                      globalSensitivity: Double,
                      epsilon: Double,
                      delta: Option[Double],
                      application: (Array[Array[Double]]) => Array[Double],
                      aggregation: (Array[Array[Double]], Double, Double) =>
                        (Array[Double], Double)): Array[Double] = {

    val observationsPerSample: Int = dataset.length / m
    val samples: Array[Array[Array[Double]]] =
      sample(dataset, m, observationsPerSample)
    val beta = epsilon / 6.0
    val sampledResults: Array[Array[Double]] = samples.map(x => application(x))
    return aggregate(sampledResults, globalSensitivity, epsilon, delta,
      aggregation)
  }

  /**
    * Performs the aggregation portion of sample - aggregate.
    *
    * @param sampledResuls     Results to be aggregated.
    * @param globalSensitivity Global sensitivity of the aggregation function.
    * @param epsilon           Base privacy parameter
    * @param delta             Failure privacy parameter
    * @param aggregation       Aggregation function with a known smooth sensitivity.
    *                          Takes three parameters:
    *                          1) The vectors to be aggregated.
    *                          2) The global sensitivity of the aggregation function.
    *                          3) Epsilon, the privacy parameter.
    *                          4) Beta value of the smooth sampling.
    * @return
    */
  def aggregate(sampledResults: Array[Array[Double]],
                globalSensitivity: Double,
                epsilon: Double,
                delta: Option[Double],
                // application: (Array[Array[Double]]) => Array[Double],
                aggregation: (Array[Array[Double]], Double, Double) =>
                  (Array[Double], Double)): Array[Double] = {

    // TODO: Upper limit on beta?
    val beta = epsilon / 6.0
    val (result: Array[Double], sensitivity: Double) =
      aggregation(sampledResults, globalSensitivity, beta)
    // d corresponding to the dimensionality of the result.
    val d = result.length
    require(d == 1)
    val alpha = beta / d

    return delta match {
      case Some(d) => throw new UnsupportedOperationException("Not supporting approximate differential privacy.")

      /**
        * Lemma 2.6: f(x) + S(x)/alpha * Z is DP
        * With location = 0 and scale = 1 the Cauchy PDF is
        * 1 / (pi * [1 + x^2])
        */
      case None => result.map(r => r + cauchy(0, 1) * sensitivity / alpha)
    }
  }

  /**
    * Adds noise to f(dataset) from smooth sensitivity to make
    * (epsilon,delta)-DP.
    *
    * @param samples           Results of f(dataset).
    * @param smoothSensitivity Smooth sensitivity of the function on the
    *                          dataset.
    * @param epsilon           Base privacy parameter
    * @param delta             Failure privacy parameter
    * @return
    */
  def addNoise(value: Array[Double],
               smoothSensitivity: Double,
               epsilon: Double,
               delta: Option[Double]): Array[Double] = {
    return delta match {
      case Some(d) =>
        throw new UnsupportedOperationException("Not implemented")
      case None =>
        value.map { (x) => x + cauchy(0, 1) * (6 / epsilon) * smoothSensitivity }
    }
  }

  def privateQuantile(Z: Seq[Double], alpha: Double, epsilon: Double,
                      bound: Double): Double = {

    require(Z.length > 1)
    require(0 < alpha && alpha < 1)
    require(epsilon > 0)
    require(bound > 0)

    val k = Z.length

    val sortedZ: IndexedSeq[Double] = (Z ++ Seq(0, bound)).map { z =>
      if (z < 0) {
        0
      } else if (z > bound) {
        bound
      } else {
        z
      }
    }.sorted.toIndexedSeq

    val y: IndexedSeq[Double] = (0 to k).map { i =>
      val z = (sortedZ(i + 1) - sortedZ(i)) *
        math.exp(-1 * epsilon * math.abs(i - alpha * k))

      val zi = sortedZ(i)
      val zi1 = sortedZ(i + 1)
      val ak = alpha * k
      println(f"SSLOG: $z = $zi1 - $zi * exp(-$epsilon * |$i - $ak|)")
      z
    }
    val ySum = y.sum
    println("SSLOG: YSUM: " + ySum)
    var current = 0.0
    val rand = math.abs(Random.nextDouble())
    var randI = Int.MinValue
    (0 to k).foreach { i =>
      current += y(i) / ySum
      if (rand < current && randI == Int.MinValue) {
        randI = i
      }
    }
    require(randI != Int.MinValue)
    val interval = sortedZ(randI + 1) - sortedZ(randI)
    return math.abs(Random.nextDouble()) * interval + sortedZ(randI)
  }

  def main(args: Array[String]) = {
    println("HELLO WORLD")
    // val data = Array(205, 224, 228, 219, 211, 223, 205, 222, 210, 195, 204,
      // 212, 225, 199, 207, 222, 202, 229, 198, 209, 231, 222, 203, 182, 191,
      // 202, 190, 217, 217, 212, 228, 199, 221, 202, 189, 199, 206, 244, 233,
      // 218, 218, 210, 208, 216, 215, 196, 220, 202, 211, 222, 214, 204, 214,
      // 231, 240, 223, 200, 190, 220, 210, 216, 194, 215, 207, 207, 212, 225,
      // 182, 186, 195, 226, 199, 222, 184, 212, 197, 221, 198, 201, 200, 204,
      // 220, 193, 208, 206, 205, 207, 194, 194, 189, 207, 208, 201, 198, 208,
      // 220, 190, 225, 222, 200, 221, 176, 186, 222, 222, 216, 214, 212, 228,
      // 187, 200, 194, 192, 215, 210, 203, 200, 208, 210, 201, 216, 190, 191,
      // 162, 184, 207, 217, 222, 212, 191, 211, 225, 220, 214, 206, 210, 226,
      // 198, 203, 209, 174, 186, 200, 217, 240, 215, 210, 205, 218, 211, 229,
      // 218, 195, 208, 217, 188, 189, 194, 197, 195, 206, 199, 208, 183, 200,
      // 218, 214, 200, 209, 208, 239, 206, 202, 227, 223, 196, 211, 205, 210,
      // 219, 203, 183, 204, 208, 194, 208, 213, 211, 203, 197, 217, 218, 190,
      // 168, 211, 212, 199, 208, 216, 218, 209, 217, 182, 190, 209, 215, 211,
      // 213, 212, 207, 213, 207, 206, 188, 205, 215, 211, 221, 201, 223, 216,
      // 188, 222, 191, 201, 224, 198, 206, 199, 204, 211, 201, 220, 207, 217,
      // 208, 217, 203, 205, 190, 201, 208, 207, 216, 213, 193, 209, 192, 205,
      // 216, 219, 199, 179, 204, 206, 214, 196, 208, 210, 192, 201, 199, 194,
      // 210, 233, 205, 215, 190, 175, 229, 194, 200, 201, 233, 181, 202, 211,
      // 240, 200, 225, 200, 205, 229, 206, 228, 242, 211, 205, 204, 206, 190,
      // 204, 202, 199, 196, 215, 182, 197, 208, 224)
    // val data = Array.fill(200000)(10000)
    // val data = Array.fill(1000)(10000)
    val data = Array(1, 1, 1, 9, 10, 10, 10, 11, 100, 100, 100)

    val dataset: Array[Array[Double]] = data.map { i =>
      Array(i.toDouble)
    }.toArray
    val ds: Array[Double] = data.sorted.map(_.toDouble)

    val minValue = 1.0
    val maxValue = 100.0

    val quantile = 0.2
    val epsilon = 0.1 / 39
    val beta = epsilon / 6.0

    val t0 = System.nanoTime()
    val (median, sensitivity) = SmoothSensitivity.quantileSmoothSensitivity(
      ds.map { d => Array[Double](d) },
      quantile,
      maxValue,
      minValue,
      beta
    )
    val t1 = System.nanoTime()

    println("n^2")
    println(median(0))
    println(sensitivity)
    println("Elapsed time: " + ((t1 - t0) / 1e9) + "s")
    println()
    /*
    println(addNoise(median, sensitivity, epsilon, None)(0))
    println(addNoise(median, sensitivity, epsilon, None)(0))
    println(addNoise(median, sensitivity, epsilon, None)(0))
    */

    val t2 = System.nanoTime()
    val (median2, sensitivity2) = quantileFastSmoothSensitivity(dataset, quantile, maxValue, minValue, beta)
    val t3 = System.nanoTime()
    // val jListValues = jList(ds, quantile, beta, maxValue, minValue, 1,
      // ds.length, 1, ds.length)
    // println(jListValues.mkString(" "))
    // println(jListValues.map(_.sensitivity).max)

    println("nlogn")
    println(median2(0))
    println(sensitivity2)
    println("Elapsed time: " + ((t3 - t2) / 1e9) + "s")
    println()
    println()
  }
}
