package com.xcs.day01_basics

/**
 * Created with IntelliJ IDEA.
 * Description:bigdata
 *
 * @Author: sky
 *          15  类型检查 和 转换
 *          枚举类
 */
object IsInstanceOf_15 extends App {

  val persion = new Persion2
  val stu = new Stu5

  // 判断对象是否为某个类型的实例
  val p_is_stu: Boolean = persion.isInstanceOf[Stu5]
  println("父类 和 子类 类型：" + p_is_stu) // false
  val stu_is_per: Boolean = stu.isInstanceOf[Persion2]
  println("子类 和 父类 类型 " + stu_is_per) // true

  // 类型转换,将 父类 转 子类
  var p = stu.asInstanceOf[Persion2]
  println(s"将 父类 强转 为 子类：${p}")

  // 获取类名
  val value: Class[Persion2] = classOf[Persion2]
  println(s"获取类名：${value}")

  println("=======枚举类=============")
  println(s"${Sex.MAN}")
}

class Persion2 {}

class Stu5 extends Persion2 {}


object Sex extends Enumeration {
  val MAN = Value(1, "男")
  val WMAN = Value(2, "女")
}


