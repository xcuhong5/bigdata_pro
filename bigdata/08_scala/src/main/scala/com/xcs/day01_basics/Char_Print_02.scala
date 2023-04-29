package com.xcs.day01_basics

/**
 * Created with IntelliJ IDEA.
 * Description:bigdata
 *
 * @Author: sky
 *          2. 字符输出
 */
object Char_Print_02 {
  def main(args: Array[String]): Unit = {
    /*
    （1）	字符串，通过+号连接
    （2）	printf 用法：字符串，通过%传值。
    （3）	字符串模板（插值字符串）：通过$获取变量值
     (4)  字符串复制  字符串 * 拷贝数量
    */
    var name = "张三"
    var age = 15
    // （1）	字符串，通过+号连接
    println("name：" + name + " 年龄：" + age)
    // （2）	printf 用法：字符串，通过%传值，%s 字符 %d 整型，占位符 顺序放入变量
    printf("name:%s age:%d ", name, age)
    // （3）	字符串模板（插值字符串）：通过$获取变量值也可以 ${name}
    println(s"name:${name} 年龄：$age")
    // (4)  字符串复制  字符串 * 拷贝数量
    println(name * 3)
  }
}
