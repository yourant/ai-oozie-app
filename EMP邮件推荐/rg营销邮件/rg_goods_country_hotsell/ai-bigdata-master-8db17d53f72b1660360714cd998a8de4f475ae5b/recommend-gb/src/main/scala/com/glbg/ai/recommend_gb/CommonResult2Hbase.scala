package com.glbg.ai.recommend_gb

import com.glbg.ai.commons_util.scala._
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat
import org.apache.hadoop.hbase.util.{Bytes, MD5Hash}
import org.apache.spark.sql.Row
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}
import org.slf4j.LoggerFactory


object CommonResult2Hbase {

  val logger = LoggerFactory.getLogger(this.getClass)
  var HBASE_TABLE_NAME: String = _
  var HIVE_TABLE_NAME: String = _
  var ROW_KEY_COL: String = _

  def main(args: Array[String]): Unit = {

    HBASE_TABLE_NAME = if (args.length > 0) args(0) else "homepage_test"
    HIVE_TABLE_NAME = if (args.length > 1) args(1) else "dw_gearbest_recommend.apl_result_rtiyv_gb_cookie_fact"
    ROW_KEY_COL = if (args.length > 2) args(2) else "0,4"

    val config = new SparkConf().setAppName("CommonResult2Hbase")
    val sc = new SparkContext(config)
    val hc = new HiveContext(sc)
    val hConf = HBaseUtils.loadConfig()
    hConf.set(TableOutputFormat.OUTPUT_TABLE, HBASE_TABLE_NAME)
    hConf.set("hbase.fs.tmp.dir", "hdfs:/user/zhanrui/hbase-staging")
    val outputPath = "hdfs:/user/zhanrui/" + HBASE_TABLE_NAME
    HdfsUtils.del(sc, outputPath)


    val sql =
      s"""SELECT  *
         |FROM  $HIVE_TABLE_NAME""".stripMargin

    val baseInfoDf = hc.sql(sql)

    val rowKeyColumnsIndexArray = ROW_KEY_COL.split(Constants.ARGS_SEPARATE)

    def getRowKey(row: Row): String = {
      val rowKey = new StringBuffer()
      for (rowKeyColumnsIndex <- rowKeyColumnsIndexArray) {
        rowKey.append(row.getString(Integer.parseInt(rowKeyColumnsIndex)))
        rowKey.append(Constants.KEY_SEPARATE)
      }
      rowKey.deleteCharAt(rowKey.length() - 1)
      rowKey.toString
    }

    val dataRDD = DataFrameUtils.toRowKeyJsonRDD(
      row => {
        MD5Hash.getMD5AsHex(Bytes.toBytes(getRowKey(row)))
      }
      , baseInfoDf).combineByKey(
      List(_),
      (x: List[String], y: String) => y :: x,
      (x: List[String], y: List[String]) => x ::: y
    )

    HFileImportUtils.importData(sc, hConf, dataRDD, outputPath, "cf", "goods_info", HBASE_TABLE_NAME, 5)

    sc.stop()


  }

}
