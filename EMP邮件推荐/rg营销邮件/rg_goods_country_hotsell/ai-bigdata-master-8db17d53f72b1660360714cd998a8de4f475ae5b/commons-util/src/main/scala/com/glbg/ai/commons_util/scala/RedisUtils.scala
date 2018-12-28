package com.glbg.ai.commons_util.scala

import java.util

import redis.clients.jedis.{HostAndPort, JedisCluster, JedisPoolConfig}


object RedisUtils {


  private val redisConfig = "redis-cluster.properties"

  //加载redis集群配置
  private val properties = ConfigUtils.getProperties(redisConfig)
  private val names = properties.propertyNames()

  def initJedisCluster(): JedisCluster = {
    val IP_INDEX = 0
    val PORT_INDEX = 1
    val nodes = new util.HashSet[HostAndPort]
    while (names.hasMoreElements) {
      val name = names.nextElement().asInstanceOf[String]
      val splitHostAndPort = properties.getProperty(name).split(":")
      val hostAndPort = new HostAndPort(splitHostAndPort(IP_INDEX),
        Integer.valueOf(splitHostAndPort(PORT_INDEX)))

      nodes.add(hostAndPort)
    }
    val poolConfig = new JedisPoolConfig()
    val jedisCluster = new JedisCluster(nodes, poolConfig)
    jedisCluster
  }

}
