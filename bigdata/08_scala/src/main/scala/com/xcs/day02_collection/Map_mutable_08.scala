package com.xcs.day02_collection

import scala.collection.mutable

/**
 * Created with IntelliJ IDEA.
 * Description:bigdata
 *
 * @Author: sky
 *          可变的 map
 */
object Map_mutable_08 extends App {
  //可变的 map
  val map: mutable.Map[String, Int] = mutable.Map("a" -> 13, "c" -> 18)
  // 添加元素 ，函数
  map.put("d", 1)
  map += (("f", 5)) // 符号的 方式
  println(s"map 集合：${map}")
  // 删除元素
  map.remove("d")
  map -= "f" // 符号  删除元素
  println(s"删除元素 d 和 f：${map}")
  // 修改元素
  map.update("a", 0)
  map("c") = -1 // 方式2
  println(s"修改元素 a 和 c：${map}")

  val map2: mutable.Map[String, Int] = mutable.Map("k" -> 2)
  map2 ++= map // 集合 合并 ，map加入到 map2
  println(s"map2 ： ${map2}")
  println(s"map1 ： ${map}")

  val bool: Boolean = map.contains("a") // 判断 元素 是否存在
  println(s"判断 元素 是否存在：${bool}")


  val toMap: Map[String, Int] = map2.toMap
  val buffer: mutable.Buffer[(String, Int)] = toMap.toBuffer

  /** ********** mapValues 遍历 key 对应 的 value ************* */
  val mapv: mutable.Map[String, List[Int]] = mutable.Map("a" -> List(1, 2, 3), "b" -> List(3, 5))
  val map0s: collection.Map[String, Int] = mapv.mapValues(_.sum)
  println("mapValues 对集合 values 操作：" + map0s)
  println()
}
