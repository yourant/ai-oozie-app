package com.glbg.ai.commons_util.scala

import org.apache.hadoop.fs.permission.FsPermission
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.SparkContext

object HdfsUtils {

  def del(sc: SparkContext, path: String): Boolean = {
    del(sc, path, true)
  }

  def del(sc: SparkContext, path: String, recursive: Boolean): Boolean = {
    val hdfs = FileSystem.newInstance(sc.hadoopConfiguration)
    hdfs.delete(new Path(path), recursive)
  }

  def chmod(sc: SparkContext, path: Path, perm: String, recursive: Boolean): Unit = {
    val hdfs = FileSystem.newInstance(sc.hadoopConfiguration)
    hdfs.setPermission(path, new FsPermission(perm))
    if (recursive && hdfs.isDirectory(path)) {
      val it = hdfs.listLocatedStatus(path)
      while (it.hasNext) {
        chmod(sc, it.next().getPath, perm, recursive)
      }
    }
  }

}
