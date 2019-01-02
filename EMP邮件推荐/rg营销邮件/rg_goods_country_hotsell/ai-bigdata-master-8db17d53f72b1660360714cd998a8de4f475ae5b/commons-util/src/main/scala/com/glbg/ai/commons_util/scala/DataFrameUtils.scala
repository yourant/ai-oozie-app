package com.glbg.ai.commons_util.scala

import com.fasterxml.jackson.core.io.JsonStringEncoder
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row}


object DataFrameUtils {

  def toRowKeyJsonRDD(rowKeygenerator: Row => String, df: DataFrame): RDD[Tuple2[String, String]] = {
    val encoder = JsonStringEncoder.getInstance()
    val colNames = df.columns.map(colName => new String(encoder.quoteAsString(colName.trim)))
    df.rdd.map(row => {
      val encoder1 = JsonStringEncoder.getInstance()
      val json = new StringBuilder("{")
      var index = 0
      colNames.foreach(colName => {
        if (index > 0) json.append(",")
        json.append("\"" + colName + "\":")
        val value = row.get(index)
        if (value == null) {
          json.append("\"\"")
        } else {
          json.append("\"" + new String(encoder1.quoteAsString(String.valueOf(value).trim)) + "\"")
        }
        index += 1
      })
      json.append("}")
      (rowKeygenerator(row), json.toString())
    })
  }

}
