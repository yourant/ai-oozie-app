package com.glbg.ai.commons_util.scala

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.hbase.client.ConnectionFactory
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.{HFileOutputFormat2, LoadIncrementalHFiles, TableOutputFormat}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{KeyValue, TableName}
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.rdd.RDD
import org.apache.spark.{Partitioner, SparkContext}
import org.json.JSONArray
import org.slf4j.LoggerFactory

object HFileImportUtils {

  private val logger = LoggerFactory.getLogger(this.getClass)

  /** *
    * 要保证处于同一个region的数据在同一个partition里面，那么首先我们需要得到table的startkeys
    * 再根据startKey建立一个分区器
    * 分区器有两个关键的方法需要去实现
    *  1. numPartitions 多少个分区
    *  2. getPartition给一个key，返回其应该在的分区  分区器如下：
    *
    * 参数splits为table的startKeys
    * 参数numFilesPerRegion为一个region想要生成多少个hfile，便于理解  先将其设置为1 即一个region生成一个hfile
    * h可以理解为它在这个region中的第几个hfile（当需要一个region有多个hfile的时候）
    * 因为startKeys是递增的，所以找到第一个大于key的region，那么其上一个region，这是这个key所在的region
    */
  private class HFilePartitioner(conf: Configuration, splits: Array[Array[Byte]], numFilesPerRegion: Int) extends Partitioner {
    private val fraction = 1 max numFilesPerRegion min 128

    override def getPartition(key: Any): Int = {
      def bytes(n: Any) = n match {
        case s: String => Bytes.toBytes(s)
        case s: Long => Bytes.toBytes(s)
        case s: Int => Bytes.toBytes(s)
      }

      val h = (key.hashCode() & Int.MaxValue) % fraction
      for (i <- 1 until splits.length)
        if (Bytes.compareTo(bytes(key), splits(i)) < 0) return (i - 1) * fraction + h

      (splits.length - 1) * fraction + h
    }

    override def numPartitions: Int = splits.length * fraction
  }

  /**
    * 将DataFrame通过HFile导入HBase
    */
  def importData(sc: SparkContext, hConf: Configuration, rdd: RDD[(String, List[String])], hFilePath: String, cf: String, col: String, tableName: String, hFileNum: Int): Unit = {

    hConf.set(TableOutputFormat.OUTPUT_TABLE, tableName)

    // 获取hbase连接
    val connection = ConnectionFactory.createConnection(hConf)
    val admin = connection.getAdmin
    val hbTableName = TableName.valueOf(tableName)
    // 获取指定表
    val realTable = connection.getTable(hbTableName)

    // 获取hbase的region
    val regionLocator = connection.getRegionLocator(hbTableName)

    //创建Hfile 相关rdd，根据Hbase分区做repartition
    logger.info("Start to build hbase rdd and do sort & repartition.")
    val hbaseRdd = rdd.repartitionAndSortWithinPartitions(new HFilePartitioner(hConf, regionLocator.getStartKeys, hFileNum))
      .map { x =>
        val kv: KeyValue = new KeyValue(Bytes.toBytes(x._1), Bytes.toBytes(cf), Bytes.toBytes(col), Bytes.toBytes(new JSONArray(x._2.toArray).toString))
        (new ImmutableBytesWritable, kv)
      }
    logger.info("Build hbase rdd finished.")


    val job = Job.getInstance(hConf)
    job.setOutputKeyClass(classOf[ImmutableBytesWritable])
    job.setOutputValueClass(classOf[KeyValue])
    HFileOutputFormat2.configureIncrementalLoad(job, realTable, regionLocator)

    //Hfile 结果保存在HDFS上如果目录存在先将其删除
    HdfsUtils.del(sc, hFilePath)

    //写入到hfile数据
    logger.info("Start to write hfiles to HDFS.")
    hbaseRdd.saveAsNewAPIHadoopFile(hFilePath, classOf[ImmutableBytesWritable], classOf[KeyValue],
      classOf[HFileOutputFormat2], job.getConfiguration)

    //文件目录权限修改
    HdfsUtils.chmod(sc, new Path(hFilePath), "777", true)
    logger.info("Write hfiles to HDFS finished.")

    //将保存在临时文件夹的hfile数据保存到hbase中
    logger.info("Start to load hfiles to HBASE.")
    val bulkLoader = new LoadIncrementalHFiles(hConf)
    bulkLoader.doBulkLoad(new Path(hFilePath), admin, realTable, regionLocator)
    logger.info("Load hfiles to HBASE finished.")

  }

}
