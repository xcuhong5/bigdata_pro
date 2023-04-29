package com.xcs.day01_basics

import scala.collection.immutable
import scala.util.control.Breaks

/**
 * Created with IntelliJ IDEA.
 * Description:bigdata
 *
 * @Author: sky
 *          7. 流程控制
 *          if  else
 *          for
 *          while
 *          Breaks.breakable
 */
object ProcessControl_07 {
  def main(args: Array[String]): Unit = {
    /*
    * if else
    */
    val age = 15
    // if 是有返回值的 ，返回值就是执行的最后一行，返回的 类型不统一时，推导是 Any
    val result = if (age >= 10 && age <= 20) {
      s"${age} 岁青年！"
    } else if (age >= 20 && age <= 30) {
      s"${age} 岁 壮年！"
    } else {
      age
    }
    println(s"if 的返回值 ： ${result}")

    /*
    * for 循环
    * i 是变量 , 循环 1-5
    */
    for (i <- 1 to 5) {
      println(s"我是 范围 循环..${i}")
    }

    // 1 到 5-1的范围，until 不包含 右边界
    for (i <- 1 until 5) {
      println(s"我是 范围 循环..${i}")
    }

    // for 循环 守卫模式, if 条件是true 的进入循环, 相当于continue
    for (i <- 1 to 10 if i % 2 != 0) {
      println(s"循环守卫，输出奇数：${i}")
    }

    // for 遍历集合
    for (i <- Array(32, 88, 66, "name")) {
      println(s"遍历 集合 元素：${i}")
    }

    println("======= for 步长===========")
    // for 步长, 隔3个元素 取一次
    for (i <- 1 to 10 by 3) {
      println(s"for 步长：${i}")
    }

    // for 使用步长 实现 ，倒序遍历
    for (i <- 10 to 1 by -1) {
      println(s"for 步长,倒序输出：${i}")
    }
    // 0.5 步长，要求 都是 double 类型
    for (i <- 1.0 to 5.0 by 0.5) {
      println(s"for 步长0.5：${i}")
    }

    // reverse 反转， 实现倒序 遍历
    for (i <- 1 to 5 reverse) {
      println(s"reverse 倒序遍历：${i}")
    }

    println("======= for 嵌套 双循环===========")
    // for 嵌套，双循环,也可以 java那样 写两层for
    for (i <- 1 to 5; j <- 1 to 2) {
      println(s"i = ${i},j = ${j}")
    }

    println("======= for 乘法表 ===========")
    // for 实现 9*9乘法表
    for (i <- 1 to 9; j <- 1 to i) {
      print(s"${j} * ${i} = ${i * j}   ")
      if (j == i) {
        println()
      }
    }

    // for 中 引入变量,等价于 在循环体中写 var j = 5-i
    for (i <- 1 to 5; j = 5 - i) {
      println(s"i = ${i};j = ${j}")
    }

    println("=======for 实现 金字塔===========")
    for (i <- 1 to 9; j = 2 * i - 1; k = 9 - i) {
      println(" " * k + "*" * j)
    }

    // for 循环 返回值，返回的 是一个 集合; 可以 用 变量 或者 函数 接收
    val strings: immutable.IndexedSeq[String] = for (i <- 1 to 5) yield i + "for 返回值"
    println(s"yield for返回值：${strings}")


    /*
      while; 一般不推荐 使用，while 是没有返回值的，
         需要通过外部变量来计算返回结果
    */
    var s = 0
    while (s < 5) {
      println(s"while ：${s}")
      s += 1
    }
    // do while; 是先循环 在判断
    var d = 0
    do {
      println(s"do while ：${d}")
      d += 1
    } while (d < 5)

    /*
      循环 中断； breakable 控制流程 来实现 break 和 continue
        continue 可以通过 守卫模式 实现
      原理 是 跳出的 breakable 而不是循环，底层是抛了一个异常
     */
    Breaks.breakable(
      for (i <- 1 to 5) {
        println(i)
        if (i == 3) {
          // 跳出 循环
          Breaks.break()
        }
        println("正常")
      }
    )
    // breakable 控制 实现 continue；Breaks.breakable  卸载里面
    for (i <- 1 to 10) {
      Breaks.breakable {
        if (i % 2 == 0) {
          // 是偶数 就跳出 当次循环
          Breaks.break()
        }
        println(i)
      }
    }


  }
}
