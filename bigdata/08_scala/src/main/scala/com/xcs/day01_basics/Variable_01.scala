package com.xcs.day01_basics

/**
 * Created with IntelliJ IDEA.
 * Description:bigdata
 *
 * @Author: sky
 *        1.  scala 的 变量 var 常量 val
 */
object Variable_01 {
  /*（1）	声明变量时，类型可以省略，编译器自动推导，即类型推导
    （2）	类型确定后，就不能修改，说明 Scala 是强数据类型语言。
    （3）	变量声明时，必须要有初始值
    （4）	在声明/定义一个变量时，可以使用 var 或者 val 来修饰，var 修饰的变量可改变， val 修饰的变量不可改。
  */
  def main(args: Array[String]): Unit = {
    var addr: String = "chengdu"
    // 数据类型 可以省略，scala 自动 推导数据类型
    var age = 18
    var name = "lisa"
    // 声明一个常量
    val sex = "man"
    /*
      如果 常量 是引用类型，引用指向的对象是不能变的，但是指向的对象 可以更改属性
      此处 student 修饰是常量，不能在 student = new Student("李四", 20) 改变student指向的引用
      可以 对象.属性名 改变对象的 属性
   */
    val student = new Student("王五", 15)
    student.name = "李四"
    student.age = 20
  }
}
