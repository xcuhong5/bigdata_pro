package com.xcs.day01_basics

/**
 * Created with IntelliJ IDEA.
 * Description:bigdata
 *
 * @Author: sky
 *          4.数据类型
 *          scala 中一切都是对象，都是 Any 的子类
 *          数据类型分为两类  AnyVal（数据类型）  和   AnyRef（引用类型），不管是值类型 还是引用类型都是对象
 *          AnyVal 数据类型：Byte、Short、Int、Long、Double、Flout、Unit、StringOps、Char、Boolean
 *          AnyRef 引用类型： scala collections、java class 、scala class、null
 *
 *          scala 依然遵循 低精度向高精度 类型自动转换（隐式转换）
 *          StringOps 是对 java 中string 的增强
 *          Unit 表示方法没有返回值，对标 java void
 *          null 是 引用类型 AnyRef的 子类，空引用
 *          Nothing 是所有类型的子类，当函数没有明确返回值 或者异常 时使用（理解成返回的不知道是什么），将返回值抛给 变量 或者 函数；
 *            可以理解为 任何返回 都可以用Nothing 接收
 */
object DataType_04 {
  /*
    和 java 8 大数据类型一样
    整数类型（Byte、Short、Int、Long）
    浮点型 （Float、Double）
    字符类型 （Char）
    布尔类型 （Boolean）

    Unit 类型 没有返回值
    Null 类型 实例对象 为空，值类型不能用
    Nothing 类型 (所有类型的子类)，返回异常，不知道会不会有返回，不知道返回什么时 用nothing；
    	可以理解为 任何返回 都可以用Nothing 接收
  */
  def main(args: Array[String]): Unit = {
    // 整数型： 一般都是用 int类型
    var num_int: Int = 10
    // 浮点型 ： 高精度小数 用double
    var num_double: Double = 8.2355
    var num_flout: Float = 02F

    /*（1）	字符常量是用单引号 ' ' 括起来的单个字符。
          特殊字符 表示
    （2）	\t ：一个制表位，实现对齐的功能
    （3）	\n ：换行符
    （4）	\\ ：表示\
    （5）	\" ：表示 " 转译
    */
    var str_char: Char = 'a'
    var spance = '\t'
    // 布尔 类型
    var b = true

    // null 空引用 实现， student 实例 为 空
    //var i: Int = null //错误的 这是数据类型
    var stu = new Student("", 0)
    stu = null

  }


}
