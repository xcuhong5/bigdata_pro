package com.spark_core.day07_architecture.common

/**
 * Created with IntelliJ IDEA.
 * Description:bigdata
 *
 * @Author: sky
 *          将 controller 的通用代码 抽取出来
 */
trait TController {

  def dispatch(): Unit

}
