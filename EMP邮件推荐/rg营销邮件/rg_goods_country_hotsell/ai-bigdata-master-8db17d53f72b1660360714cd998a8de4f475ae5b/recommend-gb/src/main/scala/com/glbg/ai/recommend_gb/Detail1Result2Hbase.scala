package com.glbg.ai.recommend_gb

import com.glbg.ai.commons_util.scala._
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat
import org.apache.hadoop.hbase.util.{Bytes, MD5Hash}
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}
import org.slf4j.LoggerFactory


object Detail1Result2Hbase {

  private val logger = LoggerFactory.getLogger(this.getClass)
  var HBASE_TABLE_NAME: String = _
  var HIVE_TABLE_NAME: String = _
  var ADD_TIME: String = _

  def main(args: Array[String]): Unit = {

    HBASE_TABLE_NAME = if (args.length > 0) args(0) else "apl_gb_result_detail_page_1_abtest1_fact"
    ADD_TIME = if (args.length > 1) args(1) else "20181024"
    HIVE_TABLE_NAME = if (args.length > 2) args(2) else "dw_gearbest_recommend.gb_result_detail_1_page_gtq"

    val config = new SparkConf().setAppName("Detail1Result2Hbase")
    val sc = new SparkContext(config)
    val hc = new HiveContext(sc)
    val hConf = HBaseUtils.loadConfig()
    hConf.set(TableOutputFormat.OUTPUT_TABLE, HBASE_TABLE_NAME)
    hConf.set("hbase.fs.tmp.dir", "hdfs:/user/zhanrui/hbase-staging")
    val outputPath = "hdfs:/user/zhanrui/" + HBASE_TABLE_NAME
    HdfsUtils.del(sc, outputPath)

    logger.info("Start to load hive table.")
    val sql =
      s"""
         |SELECT
         |	t1.goods_sn1 AS good_sn1,
         |	t1.goods_sn2 AS good_sn2,
         |	t3.pipeline_code AS pipeline_code,
         |	t3.goods_web_sku AS webgoodsn,
         |	t3.good_title AS goodstitle,
         |	t3.lang AS lang,
         |	t3.v_wh_code AS warecode,
         |	t3.total_num AS reviewcount,
         |	t3.avg_score AS avgrate,
         |	t3.shop_price AS shopprice,
         |	t3.total_favorite AS favoritecount,
         |	t3.stock_qty AS goodsnum,
         |	t3.img_url AS imgurl,
         |	t3.grid_url AS gridurl,
         |	t3.thumb_url AS thumburl,
         |	t3.thumb_extend_url AS thumbextendurl,
         |	t3.url_title AS url_title,
         |	t1.score AS score
         |FROM
         |	(
         |		SELECT
         |			goods_sn1,
         |			goods_sn2,
         |			score
         |		FROM
         |			$HIVE_TABLE_NAME
         |		WHERE
         |			concat(YEAR, MONTH, DAY) = '$ADD_TIME'
         |	) t1
         |JOIN dw_gearbest_recommend.goods_info_result_uniqlang t3 ON t1.goods_sn2 = t3.good_sn
         |""".stripMargin

    val baseInfoDf = hc.sql(sql)
    logger.info("Load hive table finished.")


    val dataRDD = DataFrameUtils.toRowKeyJsonRDD(
      row => {
        MD5Hash.getMD5AsHex(Bytes.toBytes(row.getString(0) + Constants.KEY_SEPARATE + row.getString(2) + Constants.KEY_SEPARATE + row.getString(5)))
      }
      , baseInfoDf).combineByKey(
      List(_),
      (x: List[String], y: String) => y :: x,
      (x: List[String], y: List[String]) => x ::: y
    ).map(p => (p._1, SortUtil.listSort(p._2)))

    HFileImportUtils.importData(sc, hConf, dataRDD, outputPath, "cf", "goods_info", HBASE_TABLE_NAME, 5)

    sc.stop()
  }

}
