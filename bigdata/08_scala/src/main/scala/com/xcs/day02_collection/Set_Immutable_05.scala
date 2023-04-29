package com.xcs.day02_collection


/**
 * Created with IntelliJ IDEA.
 * Description:bigdata
 *
 * @Author: sky
 *          不可变 set，无序 不重复
 */
object Set_Immutable_05 extends App {
  // 创建 不可变 的set
  val sets: Set[Int] = Set(10, 12)
  println(s"不可变 set ： ${sets}")
  // 获取元素
  println(sets)
  // 添加元素
  val set2: Set[Int] = sets + 13
  println(s"set 添加元素：${set2}")
  // 集合 合并
  val set3: Set[Int] = sets ++ set2
  println(s"set 集合 合并：${set3}")
  // set 删除 ，因为是无序的 只能 按照 值删除
  val ints: Set[Int] = set3 - 10
  println(s"set 删除 元素：${ints} ")
  // 遍历 集合
  for (elem <- ints) {
    println("set 遍历：" + elem)
  }

}
