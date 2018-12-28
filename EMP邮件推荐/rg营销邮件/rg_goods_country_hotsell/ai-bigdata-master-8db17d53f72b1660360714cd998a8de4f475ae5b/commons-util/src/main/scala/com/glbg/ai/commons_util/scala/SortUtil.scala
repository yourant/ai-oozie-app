package com.glbg.ai.commons_util.scala

import com.glbg.ai.commons_util.java.CommonUtil

object SortUtil {

  def listSort(l: List[String]): List[String] = {

    implicit val KeyOrdering = new Ordering[String] {
      override def compare(x: String, y: String): Int = {

        val x1 = CommonUtil.getJsonValue("score", x)
        val y1 = CommonUtil.getJsonValue("score", y)
        x1.compareTo(y1)
      }
    }

    l.sorted
  }

}
