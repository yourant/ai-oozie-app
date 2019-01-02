package com.glbg.ai.commons_util.scala

import org.apache.hadoop.hbase.client.Connection

/**
  * 封装HBase connection用于广播
  * @param createProducer
  */
class HBaseSink(createProducer: () => Connection) extends Serializable {
  lazy val conn = createProducer()
}

object HBaseSink{
  def apply(): HBaseSink ={
    val createProducerFunc = () => {
      HBaseUtils.getConnection()
    }
    new HBaseSink(createProducerFunc)
  }
}
