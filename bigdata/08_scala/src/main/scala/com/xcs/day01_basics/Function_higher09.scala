package com.xcs.day01_basics

/**
 * Created with IntelliJ IDEA.
 * Description:bigdata
 *
 * @Author: sky
 *          高级函数
 *          柯力化 和 闭包
 *          闭包： 内部函数 调用 外部的 局部变量，形成闭包
 *          柯力化：一个参数列表，变成多个参数列表，def test(a:Int)(b:Int)
 *          闭包的 形式 可以用柯力化实现，柯力化的形式一定是闭包
 *          递归
 *          控制抽象：传名参数，将 代码块作为参数
 *          懒加载
 */
object Function_higher09 extends App {
  // 柯力化 +  闭包；底层是 第一层参数a 第二层 参数b，b调用外部变量a
  def add(a: Int)(b: Int) = {
    a + b
  }

  println(s"柯力化 + 闭包：${add(3)(4)}")

  /*
  递归： 5*4*3*2*1
   */
  def dg(a: Int): Int = {
    if (a == 1) return 1
    return a * dg(a - 1)
  }

  println(s"递归函数：${dg(5)}")

  /*
  控制抽象： 传名参数，将代码块 作为参数
   */
  def obj(a: => Int): Unit = {
    println(s"控制抽象：${a}")
    println(s"控制抽象：${a}")
  }
  // 将代码块 当做参数
  obj(18)
  obj({
    println("这是一个代码块")
    16
  })

  /*
  懒加载：lazy 修饰，被调用时 才会 加载
   */
  def sum(a: Int, b: Int): Int = {
    println("调用3")
    a + b
  }
  // 懒加载，调用sum2 时 才会 加载 此代码的过程
  lazy val sum2 = sum(1, 2)

  println(s"sum:${sum2}")
}
