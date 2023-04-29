package com.xcs.day01_basics

import scala.beans.BeanProperty

/**
 * Created with IntelliJ IDEA.
 * Description:bigdata
 *
 * @Author: sky
 *          11 类和对象  -> 构造器
 *
 *          11-1 样列类
 *          就是一个样板的意思，使用case 修饰的类 类属性在主构造，实例化时自动封装类的方法，
 *          如：get set tostring，Serializable,apply，equals,hashcode,copy等等
 *          case class 和 class的区别 详情--   https://blog.csdn.net/mmmmmmm_2niu/article/details/78492155?locationNum=2&fps=1
 *          直接实例化 即可使用，封装规则和普通定义的一样
 *
 *          私有构造 外部不能实例化
 *          private(var name: String, var age: Int)
 *          “@BeanProperty var name: String”  申明，可供java调用，getset
 *
 *          11-2 类 别名 => 修饰，在类 第一行声明
 *          * 嵌套类  ----   内部类
 *          scala 的 内部类构造    new 外部类. 内部类
 *          java 的内部类构造     外部类.new 内部类()
 *          类的 别名  inside =>  别名为 inside  用=> 符号修饰
 */
object Class_Object_11 extends App {
  // 实例化 对象
  val stu = new Stu
  stu.age = 18
  stu.name = "lisa"
  println(stu.age)
  stu.say()
  println("=======构造器=========")
  val stu1 = new Stu2() // 调用主构造
  val stu2 = new Stu2("sky") // 调用主构造，然后辅助构造1 赋值sky
  val stu3 = new Stu2("bb", 18) // 调用主构造，然后辅助构造2 赋值bb，然后调用辅助构造3 赋值bb 18

  val stu4 = new Stu3("李思", 15, "北大")
  println(s"有参主构造：${stu4.name},${stu4.age},${stu4.school}")

}

// 定义 一个 类  @BeanProperty 可以被njava调用，自动封装 bane 规范，get set 方法
class Stu {
  // _ 代表默认值，空值
  @BeanProperty
  var name: String = _
  @BeanProperty
  var age: Int = _
  val school: String = "hongkong"
  @BeanProperty
  var addr: String = _

  // 类 方法
  def say(): Unit = {
    println(s"${name},${age},${school},${addr}")
  }
}

/*
    scala class 的 构造器 有 主构造和辅助构造，辅助构造 方法名为this
    主构造 在 class(参数列表){}声明，无参可以省略()
    辅助构造 必须先调用 主构造
 */
class Stu2 { //无参 主构造
  // _ 代表默认值，空值
  var name: String = _
  var age: Int = _
  println("1.主构造 被调用")

  // 辅助构造1
  def this(name: String) {
    // 调用主构造
    this()
    println("2.辅助构造被调用")
    this.name = name
    println(s"2. ${name},${age}")
  }

  // 辅助构造2
  def this(name: String, age: Int) {
    // 辅助构造1 中已有主构造
    this(name)
    println("3.辅助构造被调用")
    this.age = age
    println(s"3. ${name},${age}")

  }
}

// 有参 主构造
class Stu3(var name: String, var age: Int) {
  // 此处属性 和 主构造属性 没区别
  var school: String = _

  //辅助构造
  def this(name: String, age: Int, school: String) {
    // 辅助构造，必须要调用主构造
    this(name, age)
    this.school = school
  }
}

/** ***样例类** */
case class Stu_case(@BeanProperty var name: String, var age: Int = -1, val tmp: Int = -1)

//如果要在样例类 增加方法函数
case class Stu_case2(id: Int, name: String) {
  def stuplay(id: Int) = {
    println(s"id:$id")
  }
}

/** ******类 别名 => ********** */
/*
 *    嵌套类  ----   内部类
 *          scala 的 内部类构造    new 外部类. 内部类
 *          java 的内部类构造     外部类.new 内部类()
 *          类的 别名  inside =>  别名为 inside  用=> 符号修饰
 */
class inside_class(name: String, age: Int) {
  /* 给类申明一个别名，代表this，Day04_inside_class.this
   第一行开始处申明*/
  inside => //  =>  修饰符， inside 是 Day04_inside_class.this 的别名
  println(name + "==>" + age)

  //内部类
  class addr(var addName: String) {
    // 使用外部类别名 调用外部方法
    inside.px
    // 非外部类别名的方式调用，等价于  inside.px
    inside_class.this.px

    def this() {
      this("")
      px
    }

    def px: Unit = {
      println("内部方法：" + addName)
    }

    override def toString = s"addr($addName)"
  }

  def px: Unit = {
    println("外部类方法：" + name + "==>" + age)
  }

  override def toString = s"inside_class($name,$age)"
}

object Test_inside2 {
  def main(args: Array[String]): Unit = {
    //构造外部类
    val out = new inside_class("sky", 20)
    //外部类 . 内部类，的方式构造内部类
    val in = new out.addr("hongkong")
    val no = new out.addr()
    println("外部构造：" + out)
    println("内部构造：" + in)
    println("内部无参构造：" + no)
  }
}