package com.xcs.day01_basics

/**
 * Created with IntelliJ IDEA.
 * Description:bigdata
 *
 * @Author: sky
 *        12  继承
 *          （1）	子类继承父类的属性和方法 ,scala 是单继承
 *          （2）	继承的调用顺序：父类构造器->子类构造器
 *
 */
object Extends_12 extends App {
  val emp = new Emp("张三", "四川", "001")
}

// 父类
class Person {
  var name: String = _
  // 属性 和 主构造属性 没区别
  var addr: String = _

  //辅助构造器
  def this(name: String, addr: String) {
    this()
    this.addr = addr
    this.name = name
  }
}

// 子类, 主构造参数 无修饰符，参数名和 父类参数一致
class Emp(name: String, addr: String) extends Person {
  // 子类属性
  var num: String = _

  def this(name: String, addr: String, num: String) {
    this(name, addr) // 调用父类的 构造
    this.num = num
  }

}