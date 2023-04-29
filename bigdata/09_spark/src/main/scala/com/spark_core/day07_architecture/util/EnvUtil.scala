package com.spark_core.day07_architecture.util

import org.apache.spark.SparkContext

/**
 * Created with IntelliJ IDEA.
 * Description:bigdata
 *
 * @Author: sky
 *          环境工具类
 */
object EnvUtil {

  private val scLocal: ThreadLocal[SparkContext] = new ThreadLocal[SparkContext]()

  /*
  * 经过 特质的 形式 抽取 公共方法，sparkcontext 上下文 sc会冗余
  * 所以 将 sc 通过 线程 的内存空间 传递
  */
  def putSc(sc: SparkContext) = {
    scLocal.set(sc)
  }

  // 获取 spark上下文
  def getSc() = {
    scLocal.get()
  }

  // 清楚 信息
  def clearSc() = {
    scLocal.remove()
  }

}
