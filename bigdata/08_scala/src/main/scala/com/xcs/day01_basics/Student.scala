package com.xcs.day01_basics

/**
 * Created with IntelliJ IDEA.
 * Description:bigdata
 *
 * @Author: sky
 *          伴生对象，object声明是一个单例对象，全局唯一，
 *          伴生类 和 伴生对象 名字相同，相互调用属性
 */
// 伴生类
class Student(var name: String, var age: Int) {
  // 自定义函数 调用 伴生对象 的 静态属性 school
  def printInfo(): Unit = {
    println(name + " " + age + " " + Student.school)
  }
}

// 引入伴生对象
object Student {
  // 一般类的 静态属性 声明在 伴生对象中
  val school: String = "四川大学"

  def main(args: Array[String]): Unit = {
    // 创建 对象
    val student = new Student("李大", 15)
    student.printInfo()
  }
}