package com.spark_core.day07_architecture.common

import com.spark_core.day07_architecture.util.EnvUtil
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Created with IntelliJ IDEA.
 * Description:bigdata
 *
 * @Author: sky
 *          特质：抽取 出 公共 代码
 */
trait TApplication {

  // 公共的 环境启动 方法
  def start(master: String = "local[*]", appName: String = "app")(op: => Unit) = {
    // 创建 spark 配置对象
    val sparkConf: SparkConf = new SparkConf().setMaster(master).setAppName(appName)
    val sc: SparkContext = new SparkContext(sparkConf)
    // 将 spark 上下文 添加到 线程 空间中
    EnvUtil.putSc(sc)

    // 特质 调用，实现 spark 环境 代码 公共调用，不同的app 传入不同的 逻辑即可
    try {
      op
    } catch {
      case ex => println(ex.getMessage)
    }
    sc.stop()
    // 线程 结束 清空 线程 信息
    EnvUtil.clearSc()
  }
}
