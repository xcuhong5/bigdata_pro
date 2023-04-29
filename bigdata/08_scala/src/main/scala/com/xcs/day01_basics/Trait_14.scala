package com.xcs.day01_basics



/**
 * Created with IntelliJ IDEA.
 * Description:bigdata
 *
 * @Author: sky
 *          特征 ： 接口
 *          通过 extends 继承特征，多个 就with
 */
object Trait_14 extends App {
  val user = new User
  user.sayLanguge("德语")
}

//定义一个特征（可以看做 接口和抽象类，被继承）
trait Languge {
  //声明特征变量，抽象字段，子类继承对其实现
  var say: String

  //申明抽象方法
  def sayLanguge(say: String)

  //聚合方法
  def sayLanuage2(say: String): Unit = {
    println("特征中的聚合方法：" + say)
  }

  def syas: Unit = {
    println(say)
  }
}

class User extends Languge {
  override var say: String = _

  override def sayLanguge(say: String): Unit = {
    println(s"say ${say}")
  }
}