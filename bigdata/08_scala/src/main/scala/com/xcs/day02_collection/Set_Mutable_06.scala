package com.xcs.day02_collection

import scala.collection.mutable

/**
 * Created with IntelliJ IDEA.
 * Description:bigdata
 *
 * @Author: sky
 *          可变的 mutable.Set； 无序 可重复
 */
object Set_Mutable_06 extends App {
  // 创建 可变 set
  val set1: mutable.Set[Int] = mutable.Set(5, 6, 7)
  val set2: mutable.Set[Int] = mutable.Set(5, 8, 9)
  // 添加 元素
  set1.add(18)
  println(s"可变 set ： ${set1}")
  // 删除 元素
  set1.remove(18)
  println(s"删除 set 元素： ${set1}")
  // 集合 合并
  val ints: mutable.Set[Int] = set1 ++ set2
  println(s"set 集合 合并 ：${ints}")
  // 合并集合 到 某一个 集合
  set1 ++= set2
  println(set1)

}
