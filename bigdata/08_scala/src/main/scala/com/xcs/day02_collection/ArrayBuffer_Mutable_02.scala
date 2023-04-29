package com.xcs.day02_collection

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

/**
 * Created with IntelliJ IDEA.
 * Description:bigdata
 *
 * @Author: sky
 *          可变数组 mutable;   ArrayBuffer
 *          不可变 数组 Array   可变数组  ArrayBuffer / 数组 有序可重复
 */
object ArrayBuffer_Mutable_02 extends App {
  // 方式1 ： 声明 长度
  val buffer = new ArrayBuffer[Int](3)
  // 方式2 ： 调用 apply 方法
  val buffer1: ArrayBuffer[Int] = ArrayBuffer(1, 3, 5)
  // 获取 元素
  println("获取 可变集合元素：" + buffer1(0))
  // 修改 元素
  buffer1(0) = 3
  println(s"可变数组：${buffer1.mkString(",")}")
  // 尾部 添加元素
  buffer1 += 13
  // 头部 加元素 : 后面只能是引用
  7 +=: buffer1
  println(s"添加元素：${buffer1}")

  /* 可变元素 推荐 使用 函数的 方式 添加元素 */
  buffer1.append(88, 99, 100) //头部
  buffer1.prepend(33, 66) // 尾部
  println(s"调用函数 添加元素：${buffer1}")
  // 任意下标 下添加元素,下标1 下添加元素
  buffer1.insert(1, 88)
  println(s"指定下标添加元素：${buffer1}")
  // 将一个数组中的元素 添加到 数组中,指定 下标
  buffer1.insertAll(1, ArrayBuffer(9, 10))
  println(s"数组中添加数组：${buffer1}")
  // 添加一个数组到数组的 头部
  buffer1.prependAll(ArrayBuffer(1, 3))
  println(s"添加一个数组到数组的 头部：${buffer1}")

  /* 删除元素  根据 下标 删除*/
  buffer1.remove(3)
  println(s"删除元素：${buffer1}")
  buffer1.remove(0, 5) // 从下标0开始 删除5个
  println(s"删除元素，下标开始，删除n个：${buffer1}")
  // 根基 元素的 值 删除
  buffer1 -= 100
  println(s"根据元素的 值 删除元素：${buffer1}")

  /* Array 不可变  和  ArrayBuffer 可变 之间 的转换， 数组本身不发生变化  */
  val array: Array[Int] = buffer1.toArray // 不可变 打印是地址
  println("可变 转 不可变：" + array)
  // 可变数组 转 不可变
  val buffer2: mutable.Buffer[Int] = array.toBuffer
  println(s"不可变数组 转 可变：${buffer2}") // 可变 打印 可看见 元素

  /* 多维数组 ,声明 数组 3行4列*/
  val array1: Array[Array[Int]] = Array.ofDim[Int](3, 4)


}
