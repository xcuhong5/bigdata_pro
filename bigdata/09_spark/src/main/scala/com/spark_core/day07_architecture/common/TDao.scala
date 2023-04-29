package com.spark_core.day07_architecture.common

import com.spark_core.day07_architecture.util.EnvUtil
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

/**
 * Created with IntelliJ IDEA.
 * Description:bigdata
 *
 * @Author: sky
 *          特质 抽取 公共函数，此处是 读取 文件，只需要传一个url即可，所以将方法体一并抽取
 */
trait TDao {
  def readFile(path: String) = {
    // 从线程 空间 获取 spark 上下文
    val sc: SparkContext = EnvUtil.getSc()

    // 读取 日志数据
    val file_rdd: RDD[String] = sc.textFile(path)
    file_rdd
  }
}
