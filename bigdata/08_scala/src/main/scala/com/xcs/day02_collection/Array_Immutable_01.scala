package com.xcs.day02_collection

import com.xcs.day02_collection.ArrayBuffer_Mutable_02.buffer1

import scala.collection.mutable

/**
 * Created with IntelliJ IDEA.
 * Description:bigdata
 *
 * @Author: sky
 *          不可变集合 immutable; Array
 *          不可变 数组 Array   可变数组  ArrayBuffer / 数组 有序可重复
 */
object Array_Immutable_01 extends App {
  // 方式1： 创建 数组
  val array = new Array[Int](5)
  // 方式2： 创建 数组, 调用 数组的 apply 函数
  val ints: Array[Int] = Array(1, 2, 7)
  // 访问元素 根据下标
  println(ints(0))
  // 根据下标 修改 某个元素
  ints(0) = 6
  println(ints(0))
  // 遍历数组
  for (elem <- ints) {
    println(elem)
  }
  ints.foreach(e => {
    println(e)
  })
  // 将数组 转 字符串
  println("将数组转字符串：" + ints.mkString(","))

  // 在末尾添加元素：返回一个新的 数组
  val ints2: Array[Any] = ints.:+(6).:+(8)
  println("尾部添加元素：" + ints2.mkString(","))
  // 在 头部添加元素
  val ints3: Array[Int] = ints.+:(9)
  println("头部添加元素：" + ints3.mkString(","))
  /* 简化 写法 , 7 加到 头部 +: ， 尾部 :+ 是 9,8
  ：冒号后面只能是引用，所以 数值在前面 */
  val ints4: Array[Int] = 7 +: ints :+ 9 :+ 8
  println(s"简化 添加元素：${ints4.mkString(",")}")

  /* Array 不可变  和  ArrayBuffer 可变 之间 的转换， 数组本身不发生变化  */

  //  不可变 转  可变数组
  val buffer2: mutable.Buffer[Int] = ints4.toBuffer
  println("不可变 转 可变数组： " + buffer2)
  // 可变数组  转 不可变数组
  val arrays: Array[Int] = buffer2.toArray
  println("可变 转不可变：" + arrays)
}
