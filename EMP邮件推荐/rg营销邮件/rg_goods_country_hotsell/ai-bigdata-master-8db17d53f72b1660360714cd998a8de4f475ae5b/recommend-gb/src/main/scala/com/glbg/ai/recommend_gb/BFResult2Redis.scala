package com.glbg.ai.recommend_gb

import com.glbg.ai.commons_util.scala._
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}
import org.slf4j.LoggerFactory


object BFResult2Redis {

  private val logger = LoggerFactory.getLogger(this.getClass)
  var REDIS_KEY: String = _
  var HIVE_TABLE_NAME: String = _


  def main(args: Array[String]): Unit = {


    HIVE_TABLE_NAME = if (args.length > 0) args(0) else "dw_gearbest_recommend.bf_pipline_country"
    REDIS_KEY = if (args.length > 1) args(1) else "bf_pipline_country"

    val redisKey = REDIS_KEY
    val weeks = 7 * 24 * 60 * 60

    val config = new SparkConf().setAppName("BFResult2Redis")
    val sc = new SparkContext(config)
    val hc = new HiveContext(sc)

    val sql =
      s"""SELECT  *
         |FROM  $HIVE_TABLE_NAME""".stripMargin

    val baseInfoDf = hc.sql(sql)

    val resultRdd = baseInfoDf.rdd.map(row => (row.getString(0), row.getString(1))).groupByKey(1)

    resultRdd.foreachPartition {
      p =>
        val jedisCluster = RedisUtils.initJedisCluster()
        p.foreach {
          r =>
            jedisCluster.del(redisKey)
            jedisCluster.sadd(redisKey, r._2.toArray: _*)
            jedisCluster.expire(redisKey, weeks)
        }
    }

    sc.stop()
  }

}
