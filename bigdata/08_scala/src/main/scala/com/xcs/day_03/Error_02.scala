package com.xcs.day_03

/**
 * Created with IntelliJ IDEA.
 * Description:bigdata
 *
 * @Author: sky
 *          异常
 */
object Error_02 extends App {
  try {
    var n = 10 / 0 // 捕获 异常
  } catch {
    case aex: ArithmeticException => {
      println(s"算数 异常：${aex}")
    }
    case ex: Exception => {
      println(s"发生一般 异常！${ex}")
    }
  } finally {
    println("处理结束！")
  }


}

