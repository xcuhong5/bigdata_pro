package com.xcs.day_03

/**
 * Created with IntelliJ IDEA.
 * Description:bigdata
 *
 * @Author: sky
 *          模式匹配: 更强大 的 swich
 *          匹配 常量 ，模式守卫（匹配一个范围），类型，匹配集合 数组 元祖 ，匹配变量赋值，匹配 对象
 */
object ParttenMatch_01 extends App {
  /** *****模式匹配 匹配常量 ********* */
  val a = 10
  val b = 5

  // 变量 和match 关键字 ，实现 case匹配
  def match01(op: Char) = op match {
    case '+' => a + b
    case '-' => a - b
    case _ => "no....."
  }
  // 调用 模式匹配
  println("模式匹配 demo：" + match01('+'))
  println()

  /** *****模式匹配 守卫模式 ( 添加 一个 判断 ) 匹配一个范围******** */
  def abs(i: Int) = i match {
    case i if i > 10 => i
    case i if i < 10 => -i
    case _ => 0
  }

  println("模式匹配 守卫模式：" + abs(11))
  println()

  /** *******模式 匹配 匹配 类型**************** */
  // 匹配 常量
  def match_final(f: Any) = f match {
    case 1 => "is int"
    case true => "is boolean"
    case "hello" => "is string"
    case '+' => "is char "
    case _ => "don't know "
  }

  println(s"匹配 常量：${match_final(true)}")
  println()

  /** ************匹配 集合 *************** */
  // 匹配 类型; 底层 对集合 会有泛型 擦除，只能匹配是否是list map。。 / 数组 不擦除泛型
  def match_class(c: Any) = c match {
    case a: Int => "int : " + a // 变量声明
    case k: String => "string : " + k
    case d: Boolean => "boolean : " + d
    case e: List[String] => "List[String] :" + e // 泛型 类型 会擦除，只要 是list 都会 匹配
    case arr: Array[String] => "Array[String] :" + arr // 数组 泛型 不会擦除，会完全匹配 数组[泛型]
    case m => "don't know type:" + m
  }

  println(s"匹配 类型：${match_class(List(12))}")
  println(s"匹配 类型：${match_class(Array("d"))}")
  println()

  // 匹配 数组
  for (arr <- List(
    Array(0),
    Array(1, 0),
    Array(0, 1, 0),
    Array(1, 1, 0),
    Array(2, 3, 7, 15),
    Array("hello", 20, 30)
  )) {
    // 对遍历 的数组 元素 进行 模式匹配
    val result = arr match {
      case Array(0) => "匹配  元素只有 0 = > " + arr.mkString(",")
      case Array(1, 0) => "匹配 元素 1 和 0 = > " + arr.mkString(",")
      case Array(x, y) => s" 匹配 只有两个元素的 数组 $x,$y = > " + arr.mkString(",")
      case Array(0, _*) => s"匹配 0 元素开头的 数组 = > " + arr.mkString(",")
      case Array(x, 1, z) => s"匹配 中间为1  的三元数组 = > " + arr.mkString(",")
      case _ => "未知 匹配 => " + arr.mkString(",")
    }
    println(result)
  }
  println()

  // 匹配 list 方式1

  for (list <- List(
    List(0),
    List(1, 0),
    List(0, 1, 0),
    List(1, 1, 0),
    List(2, 3, 7, 15),
    List("hello", 20, 30)
  )) {
    // 对遍历 的数组 元素 进行 模式匹配
    val result = list match {
      case List(0) => "匹配  元素只有 0 = > " + list.mkString(",")
      case List(1, 0) => "匹配 元素 1 和 0 = > " + list.mkString(",")
      case List(x, y) => s" 匹配 只有两个元素的 数组 $x,$y = > " + list.mkString(",")
      case List(0, _*) => s"匹配 0 元素开头的 数组 = > " + list.mkString(",")
      case List(x, 1, z) => s"匹配 中间为1  的三元数组 = > " + list.mkString(",")
      case _ => "未知 匹配 => " + list.mkString(",")
    }
    println(result)
  }
  println()

  // 方式 2
  val ints = List(1, 2, 4, 5, 7)
  ints match {
    // 集合 :: 是右往左 看的 ，three 是集合 ，two 变量 是元素 加到 three，one 变量 是元素 加到 three
    case one :: two :: three => println(s"one : $one two : $two three: $three")
    case _ => "no.."
  }
  println()

  /** ****匹配 元祖************* */
  for (tuples <- List(
    (1, 0),
    (0, 1),
    (1, 1, 0),
    (2, 3, 7, 15),
    ("hello", 20, 30)
  )) {
    // 对遍历 的数组 元素 进行 模式匹配
    val result = tuples match {
      case (a, b) => s"匹配 二元 元祖 = > ($a,$b)  "
      case (1, _) => "匹配 二元 元祖 1 开头 = > (1,_)"
      case (a, 1, b) => s"匹配 三元元祖 中间是 1： ( $a,1,$b) = > "
      case _ => "未知 匹配 => "
    }
    println(result)
  }
  println()
  /** ****** 模式匹配实现  变量 声明 和 赋值************ */
  val (one, two) = (10, "one") // 声明 变量同时 赋值
  println(s"one: $one two: $two")

  val List(x, y, _*) = List(0, 1, 2, 4, 5) // _* 表示 多个
  println(s"x = $x, y = $y ") // _*  是无法 输出

  val threes :: twos :: ones = List(0, 1, 2, 4, 5) // 此时 ones 就是一个 集合了 可以接收 剩下的 元素
  println(s"threes : $threes twos : $twos  ones : $ones")
  println()

  /** ******for 推导式 进行 模式匹配*********** */
  val ints1: List[(Int, String)] = List((3, "a"), (5, "f"), (6, "k"))
  for (elem <- ints1) {
    println(elem._1 + "=>" + elem._2)
  }
  // 方式 2： 将元素 赋值变量 生成 二元元祖
  for ((k, v) <- ints1) {
    println("增加可读性：" + k + "=>" + v)
  }
  // 方式3：赛选 元素，类似 循环守卫
  for ((k, "a") <- ints1) {
    println(s"模式匹配 + 赛选第二个元素 是a 的：${k}")
  }

  println()

  /** ********匹配 对象 *********** */
  case class User(age: Int, name: String) // 样例类, 自动 实现了 apply、unapply、toString、equals、hashCode 和 copy

  val skyUser: User = User(18, "sky2")
  val res = skyUser match {
    case User(18, "sky") => "sky 18岁"
    case _ => "未知 用户"
  }
  println(s"匹配 对象的 内容：${res}")


}
