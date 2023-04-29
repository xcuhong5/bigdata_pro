package com.spark_core.day05_Acc_Broadcast

import scala.collection.mutable

/**
 * Created with IntelliJ IDEA.
 * Description:bigdata
 *
 * @Author: sky
 *          DateTime: 2023-02-15 17:14  
 */
object Test001 extends App {
  val map: mutable.Map[String, Int] = mutable.Map("a" -> 13, "c" -> 18)
  val i: Int = map.getOrElse("x", 1)
  println(i)
}
