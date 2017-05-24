package edu.columbia.cs.pyramid

import java.util

import redis.clients.jedis.{Jedis}

import scala.collection.mutable.ArrayBuffer
import scala.util.matching.Regex

/**
  * Implementation of a LabelCounter using a HashMap.
  *
  */
class DistributedLabelCounter(val name: String = "experiment",
                              val redisURL: String = "localhost") extends LabelCounter {

  private val namespace: String = "lc" + ":" + name

  private def getJedis(): Jedis = {
    return JedisPoolInstance(redisURL).getResource
  }

  private def getKeyForLabel(labelValue: Double): String = {
    return namespace + ":" + labelValue.toString
  }

  private def getKeyForTotal(): String = {
    return namespace + ":" + "total_observations"
  }

  override def increment(labelValue: Double, by: Double = 1): Unit = {
    val jedis = getJedis()
    try {
      val transaction = jedis.multi()
      val totalKey = getKeyForTotal()
      val labelKey = getKeyForLabel(labelValue)
      transaction.incrByFloat(labelKey, by)
      transaction.incrByFloat(totalKey, by)
      transaction.exec()
    } finally {
      jedis.close()
    }
  }

  override def labelProbability(labelValue: Double): Double = {
    val jedis = getJedis()
    try {
      // TODO: make it a script cause it's racy as is if we add data at the same time
      val total = jedis.get(getKeyForTotal()).toDouble
      return total match {
        case 0.0 => 0.0
        case _ => {
          val labelCount = jedis.get(getKeyForLabel(labelValue)).toDouble
          labelCount / total
        }
      }
    } finally {
      jedis.close()
    }
  }

  override def cleanup(): Unit = {
    JedisPoolInstance.destroy()
  }

  /**
    * Returns a sequence of all labels in the counter.
    *
    * @return
    */
  override def getLabels(): Seq[Double] = {
    val keyExpression = namespace + ":*"
    val jedis = getJedis()
    try {
      val retseq = new ArrayBuffer[Double]()
      val keys  = scala.collection.JavaConversions.asScalaBuffer(jedis.scan(keyExpression).getResult)
      keys.foreach{k =>
        val splits = k.split(":")
        if (splits.length >= 2) {
          retseq :+ splits(1).toDouble
        }
      }
      return retseq
    } finally {
      jedis.close()
    }
  }

  /**
    * Returns the number of times the label has been observed.
    *
    * @param label
    * @return
    */
  override def getLabelCount(label: Double): Double = {
    val k = getKeyForLabel(label)
    val jedis = getJedis()
    try {
      return jedis.get(k) match {
        case s: String => s.toDouble
        case _ => 0.0
      }
    } finally {
      jedis.close()
    }
  }
}
