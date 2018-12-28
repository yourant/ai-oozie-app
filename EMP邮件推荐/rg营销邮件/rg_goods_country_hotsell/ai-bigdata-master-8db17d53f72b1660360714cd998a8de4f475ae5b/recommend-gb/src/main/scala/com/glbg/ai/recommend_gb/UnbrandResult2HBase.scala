package com.glbg.ai.recommend_gb

import com.glbg.ai.commons_util.scala._
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat
import org.apache.hadoop.hbase.util.{Bytes, MD5Hash}
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}
import org.slf4j.LoggerFactory

object UnbrandResult2HBase {

  val logger = LoggerFactory.getLogger(this.getClass)

  def main(args: Array[String]): Unit = {
    val config = new SparkConf().setAppName("UnbrandResult2HBase")
    val sc = new SparkContext(config)
    val hc = new HiveContext(sc)
    val hConf = HBaseUtils.loadConfig()
    val hbaseTable = "apl_result_rpfy_unbrand_gb_fact"
    //val hbaseTable = "test001"
    hConf.set(TableOutputFormat.OUTPUT_TABLE, hbaseTable)
    hConf.set("hbase.fs.tmp.dir", "hdfs:/user/yanhan1/hbase-staging")
    val hfilePath = "hdfs:/user/yanhan1/gbUnbrandResult2HBase"

    val goodsInfoResult = hc.sql("""SELECT
                                   |  b.*
                                   |FROM
                                   |  dw_gearbest_recommend.pipeline_language_map a
                                   |JOIN
                                   |  dw_gearbest_recommend.goods_info_result b
                                   |on
                                   |  a.pipeline_code=b.pipeline_code and a.lang = b.lang
                                 """.stripMargin)
    goodsInfoResult.registerTempTable("goodsInfoResult")
    hc.cacheTable("goodsInfoResult")

    val spuHotSell15Days = hc.sql(
      """
        |SELECT
        |*
        |FROM dw_gearbest_recommend.goods_spu_hotsell_15days
      """.stripMargin)
    spuHotSell15Days.registerTempTable("spuHotSell15Days")
    hc.cacheTable("spuHotSell15Days")

    val level4Result = hc.sql(
      """
        |SELECT
        |   goods_spu1,
        |   goods_spu2,
        |   pipeline_code,
        |   sellcount,
        |   score
        |FROM
        |   (SELECT
        |       goods_spu1,
        |       goods_spu2,
        |       pipeline_code,
        |       sellcount,
        |       score,
        |       ROW_NUMBER() OVER(PARTITION BY goods_spu1,pipeline_code ORDER BY sellcount DESC) AS flag
        |   FROM(
        |       SELECT
        |           t1.goods_spu goods_spu1,
        |           t2.goods_spu goods_spu2,
        |           t2.pipeline_code,
        |           t2.sellcount,
        |           4 score
        |       FROM
        |         (SELECT
        |				      goods_spu,
        |				      pipeline_code,
        |				      level_4
        |			     FROM
        |				      goodsInfoResult
        |			     GROUP BY
        |				      goods_spu,
        |				      pipeline_code,
        |				      level_4
        |				  )t1
        |       JOIN
        |           spuHotSell15Days t2
        |       ON
        |           t1.level_4 = t2.category_id AND t1.pipeline_code = t2.pipeline_code
        |   )tmp
        |)t
        |WHERE flag < 31
      """.stripMargin)
    level4Result.registerTempTable("level4Result")

    val spuTmp1 = hc.sql(
      """
        |SELECT
        | t2.*
        |FROM
        | (
        |   SELECT
        |		  d.goods_spu1
        |   FROM
        |		  (SELECT
        |			  	b.pipeline_code,b.goods_spu1,COUNT(*) c
        |		  FROM
        |		      level4Result b
        |		  GROUP BY
        |		      b.goods_spu1,b.pipeline_code
        |		  HAVING
        |			    c > 29
        |		  )d
        | ) t1
        | JOIN
        |   goodsInfoResult t2
        | ON t2.goods_spu = t1.goods_spu1
        |
      """.stripMargin)
    val level4NotInTmp = goodsInfoResult.except(spuTmp1).select("goods_spu", "pipeline_code", "level_3").distinct()
    level4NotInTmp.registerTempTable("level4NotInTmp")

    val level3Result = hc.sql(
      """
        |SELECT
        |   goods_spu1,
        |   goods_spu2,
        |   pipeline_code,
        |   sellcount,
        |   score
        |FROM
        |   (SELECT
        |       goods_spu1,
        |       goods_spu2,
        |       pipeline_code,
        |       sellcount,
        |       score,
        |       ROW_NUMBER() OVER(PARTITION BY goods_spu1,pipeline_code ORDER BY sellcount DESC) AS flag
        |   FROM(
        |       SELECT
        |           t1.goods_spu goods_spu1,
        |           t2.goods_spu goods_spu2,
        |           t2.pipeline_code,
        |           t2.sellcount,
        |           3 score
        |       FROM
        |           level4NotInTmp t1
        |       JOIN
        |           spuHotSell15Days t2
        |       ON
        |           t1.level_3 = t2.category_id AND t1.pipeline_code = t2.pipeline_code
        |       )tmp
        |   )t
        |WHERE
        |   flag < 31
      """.stripMargin)
    level3Result.registerTempTable("level3Result")

    val level3And4Result = level3Result.unionAll(level4Result)
    level3And4Result.registerTempTable("level3And4Result")

    val spuTmp2 = hc.sql(
      """
        |SELECT
        | t2.*
        |FROM
        | (
        |   SELECT
        |		  d.goods_spu1
        |   FROM
        |		  (SELECT
        |			  	b.pipeline_code,b.goods_spu1,COUNT(*) c
        |		  FROM
        |		      level3And4Result b
        |		  GROUP BY
        |		      b.goods_spu1,b.pipeline_code
        |		  HAVING
        |			    c > 29
        |		  )d
        | ) t1
        | JOIN
        |   goodsInfoResult t2
        | ON t2.goods_spu = t1.goods_spu1
        |
      """.stripMargin)
    val level3NotInTmp = goodsInfoResult.except(spuTmp2).select("goods_spu", "pipeline_code", "level_2").distinct()
    level3NotInTmp.registerTempTable("level3NotInTmp")
    val level2Result = hc.sql(
      """
        |SELECT
        |   goods_spu1,
        |   goods_spu2,
        |   pipeline_code,
        |   sellcount,
        |   score
        |FROM
        |   (SELECT
        |       goods_spu1,
        |       goods_spu2,
        |       pipeline_code,
        |       sellcount,
        |       score,
        |       ROW_NUMBER() OVER(PARTITION BY goods_spu1,pipeline_code ORDER BY sellcount DESC) AS flag
        |   FROM(
        |       SELECT
        |           t1.goods_spu goods_spu1,
        |           t2.goods_spu goods_spu2,
        |           t2.pipeline_code,
        |           t2.sellcount,
        |           2 score
        |       FROM
        |           level3NotInTmp t1
        |       JOIN
        |           spuHotSell15Days t2
        |       ON
        |           t1.level_2 = t2.category_id AND t1.pipeline_code = t2.pipeline_code
        |       )tmp
        |   )t
        |WHERE
        |   flag < 31
      """.stripMargin)
    level2Result.registerTempTable("level2Result")

    val allLevelResult = level2Result.unionAll(level3And4Result)
    allLevelResult.registerTempTable("allLevelResult")

    val resultMid1 = hc.sql(
      """
        |       SELECT
        |           t1.goods_spu1,
        |           t2.good_sn,
        |           t1.pipeline_code,
        |           t3.goods_web_sku,
        |           t3.good_title,
        |           t3.lang,
        |           t3.v_wh_code,
        |           t3.total_num,
        |           t3.avg_score,
        |           t3.shop_price,
        |           t3.total_favorite,
        |           t3.stock_qty,
        |           t3.img_url,
        |           t3.grid_url,
        |           t3.thumb_url,
        |           t3.thumb_extend_url,
        |           t3.url_title
        |       FROM
        |           allLevelResult t1
        |       JOIN
        |           dw_gearbest_recommend.goods_info_mid5 t2
        |       ON
        |           t1.goods_spu2 = t2.goods_spu
        |       JOIN
        |           (
        |             SELECT
        |               a.*
        |             FROM
        |               dw_gearbest_recommend.goods_info_result_uniq a
        |             JOIN
        |               dw_gearbest_recommend.pipeline_language_map b
        |             ON
        |               a.pipeline_code=b.pipeline_code and a.lang = b.lang
        |           ) t3
        |       ON
        |           t2.good_sn = t3.good_sn AND t1.pipeline_code = t3.pipeline_code
      """.stripMargin).distinct()
    resultMid1.registerTempTable("resultMid1")

    val goodsInfoMidUniq = hc.sql(
      """
        |SELECT
        | goods_spu,
        | good_sn
        |FROM
        | goodsInfoResult
      """.stripMargin).distinct()
    goodsInfoMidUniq.registerTempTable("goodsInfoMidUniq")
    //good_sn1,good_sn2,pipeline_code,webgoodsn,goodstitle,lang,warecode,reviewcount,
    // avgrate,shopprice,favoritecount,goodsnum,imgurl,gridurl,thumburl,thumbextendurl,url_title
    val unbrandResult = hc.sql(
      """
        |SELECT
        |       t2.good_sn good_sn1,
        |       t1.good_sn good_sn2,
        |       t1.pipeline_code,
        |       t1.goods_web_sku webgoodsn,
        |       t1.good_title goodstitle,
        |       t1.lang,
        |       t1.v_wh_code warecode,
        |       t1.total_num reviewcount,
        |       t1.avg_score avgrate,
        |       t1.shop_price shopprice,
        |       t1.total_favorite favoritecount,
        |       t1.stock_qty goodsnum,
        |       t1.img_url imgurl,
        |       t1.grid_url gridurl,
        |       t1.thumb_url thumburl,
        |       t1.thumb_extend_url thumbextendurl,
        |       t1.url_title
        |   FROM
        |       goodsInfoMidUniq t2
        |   JOIN
        |       resultMid1 t1
        |   ON
        |       t1.goods_spu1 = t2.goods_spu AND t1.good_sn != t2.good_sn
      """.stripMargin)

    val dataRDD = DataFrameUtils.toRowKeyJsonRDD(
      row => {MD5Hash.getMD5AsHex(Bytes.toBytes(row.getString(0) + Constants.KEY_SEPARATE + row.getString(2) + Constants.KEY_SEPARATE + row.getString(5)))}
      , unbrandResult).combineByKey(
      List(_),
      (x: List[String], y: String) => y :: x,
      (x: List[String], y: List[String]) => x ::: y
    )

    HFileImportUtils.importData(sc, hConf, dataRDD, hfilePath, "cf","goods_info", hbaseTable, 5)

  }

}