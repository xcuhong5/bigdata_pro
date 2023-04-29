package com.spark_core.day07_architecture.appliction

import com.spark_core.day07_architecture.common.TApplication
import com.spark_core.day07_architecture.controller.WordCountController
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Created with IntelliJ IDEA.
 * Description:bigdata
 *
 * @Author: sky
 *          工程架构分层：应用层
 *          程序启动层，程序入口
 *
 */
object WordCountApp extends App with TApplication {

  // 传入 一段 代码逻辑，实现 调用方法的 公共性
  start("local[*]", "app") {
    // 调用 控制层
    val controller: WordCountController = new WordCountController()
    // 调用 调度 函数
    controller.dispatch()

  }

}
