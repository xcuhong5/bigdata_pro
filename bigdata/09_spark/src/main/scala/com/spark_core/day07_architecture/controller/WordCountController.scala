package com.spark_core.day07_architecture.controller

import com.spark_core.day07_architecture.common.TController
import com.spark_core.day07_architecture.service.WordCountService


/**
 * Created with IntelliJ IDEA.
 * Description:bigdata
 *
 * @Author: sky
 *          架构分层：控制层
 *          继承 特质，实现混入，重写 抽取的 公共方法
 */
class WordCountController extends TController {

  // 调用 业务层
  private val service: WordCountService = new WordCountService

  // 重写 特质 中公共 的 调度 函数
  def dispatch(): Unit = {
    // 调用 业务层 的数据计算 函数
    val top10_sort_rdd: Array[(String, (Int, Int, Int))] = service.dataAss()
    // 6. 将结果 输出 控制台
    top10_sort_rdd.foreach(data => println("优化后的 热门品类Top10：" + data))
  }

}
