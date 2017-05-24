package edu.columbia.cs.pyramid

import redis.clients.jedis.{JedisPool, JedisPoolConfig}

object JedisPoolInstance {
  private var jedisPool: JedisPool = null

  def apply(redisURL: String): JedisPool = {
    if (jedisPool == null) {
      this.synchronized {
        if (jedisPool == null) {
          jedisPool = new JedisPool(new JedisPoolConfig(), redisURL)
        }
      }
    }
    return jedisPool
  }

  def destroy(): Unit = {
    if (jedisPool != null) {
      jedisPool.destroy()
      jedisPool = null
    }
  }
}