package com.xcs

/**
 * Created with IntelliJ IDEA.
 * Description:bigdata
 *
 * @Author: sky
 *          object 声明 的类，是一个单例对象（伴生对象），全局唯一
 */
object HelloWord {
  // def 关键字 方法名(参数名：参数类型[泛型]):返回值（Unit空返回值）={方法体}
  def main(args: Array[String]): Unit = {
    println("scala hello word ")
    // 可以调用 java api
    System.out.println("java hello word ")
  }
}
