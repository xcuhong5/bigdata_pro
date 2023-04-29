package com.xcs.day02_collection

/**
 * Created with IntelliJ IDEA.
 * Description:bigdata
 *
 * @Author: sky
 *          元祖：一个元组最多长度22  下标从1开始，封装 不同类型的 元素
 *          有多少个 元素，就叫几元 元祖，最多 22元 元祖
 */
object Tuple_09 extends App {
  //如果一元元组 需要显性定义  Tuple1("a")
  var tuple = Tuple1("a")
  println("必须显性定义一元元组：" + tuple)
  //定义长度+类型的元组,定义元组的 类型
  var tuple3 = Tuple3[Int, String, Any](1, "b", 6)
  println("定义长度+类型的元组 :" + tuple3)
  //规定长度的元组
  var three = Tuple3(1, 2, 3)
  println("规定长度的元组 :" + three)
  //元组元>= 2 可以不用显性定义 直接 （）即可
  var tupleAny = (1, 2, 3, 4, "d")
  println(s"根据下标获取第4个元组：${tupleAny._4}")
  println("元组元>= 2 可以不用显性定义 直接 （）即可:" + tupleAny)

  //只有二元元组 才会有swap 函数 ，元素交换位置
  var two = ("a", "b")
  println("swap 互换位置 函数：" + two.swap)

  //遍历元组 需要得到元组的迭代器productIterator  再遍历
  var each = tupleAny.productIterator.foreach(x => println("遍历元组" + x))

  //拉链就是返回元组
  var idArr = Array(1, 2, 3)
  var nameArr = Array("sky", "tom", "kaka")
  var tupleArr = idArr.zip(nameArr)
  println("打印拉链元祖：" + tupleArr.toList)
  tupleArr.foreach(x => println("拉链结果：" + x))
  //将元组转map
  var map = tupleArr.toMap
  println("拉链元组转map：" + map)

  println(tuple3.getClass.getSimpleName)


}
