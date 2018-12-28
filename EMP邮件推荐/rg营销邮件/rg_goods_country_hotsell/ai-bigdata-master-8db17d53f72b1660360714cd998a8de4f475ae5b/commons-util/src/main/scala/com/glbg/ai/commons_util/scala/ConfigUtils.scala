package com.glbg.ai.commons_util.scala

import java.util.Properties

import scala.tools.nsc.interpreter.InputStream

object ConfigUtils {

  val defaultConfig = "config.properties"

  def getValue(path: String, key: String): String = {
    getProperties(path).getProperty(key)
  }

  def getValue(key: String): String = {
    getProperties(defaultConfig).getProperty(key)
  }

  def getProperties(path: String): Properties = {
    val prop = new Properties()
    prop.load(getInputStream(path))
    prop
  }

  def getInputStream(path: String): InputStream = {
    this.getClass.getClassLoader.getResourceAsStream(path)
  }

}
