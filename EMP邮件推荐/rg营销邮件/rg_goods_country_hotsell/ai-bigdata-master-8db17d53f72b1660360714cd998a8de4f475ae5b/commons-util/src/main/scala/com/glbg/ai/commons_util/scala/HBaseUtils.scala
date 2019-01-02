package com.glbg.ai.commons_util.scala

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.{Connection, ConnectionFactory}

object HBaseUtils {

  private val hbaseConfig = "hbase.properties"

  //加载hbase配置
  private val conf = HBaseConfiguration.create()
  private val properties = ConfigUtils.getProperties(hbaseConfig)
  private val names = properties.propertyNames()
  while (names.hasMoreElements) {
    val name = names.nextElement().asInstanceOf[String]
    conf.set(name, properties.getProperty(name))
  }

  //初始化hbase连接
  val connection = ConnectionFactory.createConnection(loadConfig())
  Runtime.getRuntime.addShutdownHook(new Thread() {override def run(){connection.close()}})

  def loadConfig(): Configuration = {
    conf
  }

  def getConnection(): Connection = {
    connection
  }

}
