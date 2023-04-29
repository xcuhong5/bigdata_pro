package com.xcs.day_03

import java.io.{BufferedReader, File, FileReader}
import scala.collection.mutable.ListBuffer

/**
 * Created with IntelliJ IDEA.
 * Description:bigdata
 *
 * @Author: sky
 *          隐式转换  ---  理念
 *          当对象调用类中不存在的成员或 方法时，编译器会自动将对象进行隐式转换
 *          当方法中的参数类型与目标类型不一致
 *          当方法中缺少参数时
 *          使用时 import 引入
 */
object Implicit_Demo {
  /*
  * 隐式方法：隐式转换为目标类型：将一个类型转另外一个类型
  * 隐式方法 - 将int 类型隐式转string
  * 将一个类型转另外一个类型，隐式转换为目标类型
  * 将int 隐式转为string，调用是 int 则会具备 string的 api（底层隐式将int 转了 string）
  */
  implicit def intToString(num: Int) = num.toString


  /*
   * 隐式类  - 给类扩展任意的方法  隐式类的构造参数就是需要被扩展的类 ，只能有一个参数
   * 当某个类不具备某些方法 可以通过隐式类 去扩展方方法 --- 给任意的类 扩展任意的方法
   * 定义在静态类中 object中
   * 需求 ：  给file 类 扩展一个  getLine 函数
   * 给File 类 扩展 (给类扩展方法),File作为构造参数传入，如需增加扩展方法，直接在该隐式类中加
   */
  implicit class Files(file: File) {
    //扩展的函数
    def getLine: List[String] = {
      val fileReader = new FileReader(file)
      val reader = new BufferedReader(fileReader)
      try {
        val lines = new ListBuffer[String]()
        var line = reader.readLine()
        while (line != null) {
          lines += line
          line = reader.readLine()
        }
        lines.toList
      } finally {
        if (fileReader != null) fileReader.close()
        if (reader != null) reader.close()
      }
    }
  }

  /*
   * 隐式 参数 隐式值
   * 隐式参数：implicit修饰的参数，调用时会隐式根据该参数类型  找隐式值
   * 隐式值：变量被implicit 修饰 为隐式值，一般和隐式参数混合使用
   */
  implicit val addr = "hongkong" //隐式值 和 隐式参数的 类型相同 就会被调用
  implicit val number = 18

  // 方式1 ： 根据 implicit 修饰的 参数类型 和 implicit 修饰的 隐式值 类型 相同 就会被调用
  def getAddr()(implicit address: String, num: Int) = {
    println(s"地址：${address},${num} 号")
  }

  // 方式2： 简便写法
  def getAddr2: Unit = {
    println(s"地址：${implicitly[String]}, ${implicitly[Int]} 号")
  }

}

object Implicit_03 extends App {
  // 将 隐式 类 ，方法，参数 对象的 所在类，引入，就可以 使用 隐式转换

  import Implicit_Demo._

  // int 类型 会隐式 转 string，然后 调用 string 的方法
  println(s"隐式函数：${100556.indexOf("5")}")
  println()

  // 隐式类，给file 扩展 将每一行数据 添加 到 list
  val line: List[String] = new File("08_scala/src/main/resources/Implicit_03.txt").getLine
  println(s"隐式类 ：$line")
  println()

  // 隐式参数 + 隐式值 ，两者 结合使用(空参 可以省略 括号)
  getAddr
  getAddr2
  println()



}