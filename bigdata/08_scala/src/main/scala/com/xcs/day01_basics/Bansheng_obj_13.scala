package com.xcs.day01_basics

/**
 * Created with IntelliJ IDEA.
 * Description:bigdata
 *
 * @Author: sky
 *          13 伴生对象
 *          伴生对象 名 和 伴生类名相同，是单列对象，和 伴生类可以相互访问方法和属性
 *          伴生对象中 编写 静态的属性 和 方法
 *
 */

object Bansheng_obj_13 extends App {
  // 此处创建 对象 没有new，是调用伴生对象的  apply方法
  val stu: Stu4 = Stu4("李大彪", 38)
  println(s"伴生对象：${stu.name},${stu.age}")
}

// 伴生类, 私有 构造器
class Stu4 private(var name: String, var age: Int) {}

// 伴生 对象 ，存放 伴生类 静态 属性 和 方法
object Stu4 {
  // 静态属性
  val school: String = "川大"

  /* 伴生 对象的 apply 函数，封装了new 的操作，直接调用 Stu4(name, age) 就可以创建 对象
  aooly 函数 可以让私有构造 创建对象*/
  def apply(name: String, age: Int): Stu4 = new Stu4(name, age)
}