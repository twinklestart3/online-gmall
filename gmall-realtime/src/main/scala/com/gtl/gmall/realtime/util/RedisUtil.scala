package com.gtl.gmall.realtime.util

import redis.clients.jedis.{Jedis, JedisPool, JedisPoolConfig}


object RedisUtil {
    val host: String = PropertiesUtil.getProperty("config.properties", "redis.host")
    val port: Int = PropertiesUtil.getProperty("config.properties", "redis.port").toInt

    private val jedisPoolConfig: JedisPoolConfig = new JedisPoolConfig
    jedisPoolConfig.setMaxTotal(100) // 最大连接数
    jedisPoolConfig.setMaxIdle(20) // 最大空闲数
    jedisPoolConfig.setMinIdle(20) // 最小空闲数
    jedisPoolConfig.setBlockWhenExhausted(true) // 设置忙碌时是否等待
    jedisPoolConfig.setMaxWaitMillis(500) // 忙碌时最大等待时长 毫秒
    jedisPoolConfig.setTestOnBorrow(false) // 每次获得连接时是否进行测试

    private val jedisPool: JedisPool = new JedisPool(jedisPoolConfig, host, port)

    // 直接得到一个 Redis 的连接
    def getJedisClient: Jedis = {
        jedisPool.getResource
    }
}
