package com.xcs.day02_collection

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

/**
 * Created with IntelliJ IDEA.
 * Description:bigdata
 *
 * @Author: sky
 *          可变集合 listBuffer ； 有序可重复
 */
object ListBuffer_Mutable_04 extends App {
  // 创建 可变 集合
  val listBuffer: ListBuffer[Int] = ListBuffer(15, 16)
  val listBuffer2: ListBuffer[Int] = ListBuffer(7, 3)
  // 遍历 集合
  for (elem <- listBuffer) {
    println(elem)
  }
  // 尾部添加 元素
  listBuffer.append(19, 22)
  // 头部 添加元素
  listBuffer.prepend(2, 3)
  // 指定 下标 添加 元素
  listBuffer.insert(2, 0)
  // 符号 方式 添加
  18 +=: listBuffer += 8
  println(listBuffer)

  // 追加 集合 到尾部
  listBuffer ++= listBuffer2
  // 追加 集合 到头部
  listBuffer.prependAll(listBuffer2)
  println(s"将整个集合追加 ：${listBuffer}")
  // 集合 合并   listBuffer2 ++= listBuffer 则是 合并到 listBuffer2
  val buffer: ListBuffer[Int] = listBuffer2 ++ listBuffer
  println(s"集合 合并生成 新集合：${buffer}")
  // 修改元素
  buffer.update(1, 99)
  println(s"修改元素：${buffer}")
  // 删除元素，根据下标
  buffer.remove(0)
  // 根据 元素的 值 删除
  buffer -= 3
  // 从下标 1 开始 ，删除3个元素
  buffer.remove(1, 3)
  println(s"删除元素：${buffer}")

  // 可变 list 转 不可变 list
  val list: List[Int] = buffer.toList
  // 不可变 转 可变
  val toBuffer: mutable.Buffer[Int] = list.toBuffer

}
