package com.xcs.day02_collection

/**
 * Created with IntelliJ IDEA.
 * Description:bigdata
 *
 * @Author: sky
 *          不可变 map
 */
object Map_Immtable_07 extends App {
  // 创建 不可变 map
  val map = Map("a" -> 8, "b" -> 12)
  println(s"根据key 获取元素：${map("a")}")
  // getOrElse 避免空值，如果没有该元素，返回 -1
  println(s"防止异常 ：${map.getOrElse("a", -1)}")
  // 遍历 map
  for (elem <- map) {
    println(elem._1, elem._2)
  }
  // 遍历 获取 key
  for (k <- map.keys) {
    println(s"$k ====> ${map.getOrElse(k, -1)}")
  }

  /** ***添加 删除 元素*** */
  val map1: Map[String, Int] = map + ("k" -> 2)
  println(s"不可变 集合 添加元素：${map1}")

  val map2: Map[String, Int] = map - ("b")
  println(s"不可变 集合 删除元素：${map2}")

  val map3: Map[String, Int] = map.updated("a", 20)
  println(s"不可变 集合 修改元素：${map3}")

  /** ******过滤 出 key 是k 的元素*********** */
  val map4: Map[String, Int] = map.filterKeys(_.equals("a"))
  println(s"过滤key：${map4}")

  /** ****获取 map 的 key 和 values********* */
  val keys: Iterable[String] = map.keys
  val values: Iterable[Int] = map.values

}
