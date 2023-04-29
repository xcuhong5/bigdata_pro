package com.xcs.day01_basics

/**
 * Created with IntelliJ IDEA.
 * Description:bigdata
 *
 * @Author: sky
 *          函数
 *          函数和java 方法的区别在于，java中方法只能在类中或者对象，
 *          而scala 只要是代码块中就可以声明，方法中也可以声明方法
 *          def关键字 方法名 (参数):返回值{方法体}
 *          方法声明
 *          可变参数
 *          参数默认值
 *          函数 至简原则
 *          匿名函数
 */
object Function_08 {
  /*
  * 声明 方法
  */
  def hello(name: String): Unit = {
    println(s"hello ${name}")
  }

  def main(args: Array[String]): Unit = {
    // 可以直接调用 ，也可以类名.函数名
    hello("lisa")

    /*
     函数 ，可变参数； 有参数输出的 是一个集合，不传参数 输出list()
     */
    def more_args(str: String*) = {
      println(s"可变 参数：${str}")
    }
    // 输出的 是一个集合
    more_args("name", "age")
    // 不传参数 是 list()
    more_args()

    // 固定参数 + 可变参数
    def more_args2(age: Int, str: String*) = {
      println(s"固定参数 + 可变 参数：${age}   ${str}")
    }
    // 固定参数 + 可变参数时，固定参数必填
    more_args2(15)

    /*
    参数 默认值,该参数一般放后面,否则调用时需要指定其他参数名：defult(name="张三")，默认值参数写在后面，顺序调用就不用写参数名
    函数调用时， 可以对 参数默认值进行 覆盖，不覆盖 调用时可以省略
     */
    def defult(name: String, age: Int = 15): Unit = {
      println(s"参数默认值：${name}  ${age}")
    }
    // 调用 参数默认值，不覆盖参数，可以省略
    defult("张三")
    // 覆盖 默认值
    defult("lisa", 20)

    /*      函数        至简原则，能省则省
    （1）	return 可以省略，Scala 会使用函数体的最后一行代码作为返回值
    （2）	如果函数体只有一行代码，可以省略花括号
    （3）	返回值类型如果能够推断出来，那么可以省略（:和返回值类型一起省略）
    （4）	如果有 return，则不能省略返回值类型，必须指定
    （5）	如果函数明确声明 unit，那么即使函数体中使用 return 关键字也不起作用
    （6）	Scala 如果期望是无返回值类型，可以省略等号
    （7）	如果函数无参，但是声明了参数列表，那么调用时，小括号，可加可不加
    （8）	如果函数没有参数列表，那么小括号可以省略，调用时小括号必须省略
    （9）	如果不关心名称，只关心逻辑处理，那么函数名（def）可以省略
    */
    // （1）	return 可以省略，Scala 会使用函数体的最后一行代码作为返回值
    def easy(name: String, age: Int) = {
      s"name=${name},age=${age}"
    }

    // （2）	如果函数体只有一行代码，可以省略花括号; 返回值类型如果能够推断出来，那么可以省略（:和返回值类型一起省略）
    def easy2(name: String, age: Int) = s"name=${name},age=${age}"

    // （4）	如果有 return，则不能省略返回值类型，必须指定
    def easy3(name: String, age: Int): String = {
      return s"name=${name},age=${age}"
    }

    // （6）	Scala 如果期望是无返回值类型，可以省略等号
    def easy4 {
      "easy4"
    }

    //（7）	如果函数无参，但是声明了参数列表，那么调用时，小括号，可加可不加
    def easy5() = "easy5"

    easy5

    // （9）	匿名函数：如果不关心名称，只关心逻辑处理，那么函数名（def）可以省略
    val easy6 = (name: String, age: Int) => {
      s"name=${name},age=${age}"
    }

    /*
     匿名函数: (a: Int, b: Int) => a + b
      没有名字的函数，它是一个操作，比如函数的 数据固定的，通过匿名函数的逻辑，改变数据计算的逻辑，多用于抽取公共方法用
      可以作为参数，可以用变量接收
     */
    // 声明 匿名函数 ，变量add 接收
    val add = (a: Int, b: Int) => a + b

    // 方法的参数 是函数 /  func: (Int, Int) => Int   参数名:(参数类型)=>返回值
    def app(func: (Int, Int) => Int): Int = {
      func(3, 6)
    }
    // 调用方法，将实现相加的 匿名函数 当做参数
    app(add)
    // 直接传入匿名函数，相减操作
    app((a: Int, b: Int) => a - b)
    // 简写 的 匿名函数
    app(_ + _)


    /*
      定义一个匿名函数，并将它作为值赋给变量 fun。函数有三个参数，类型分别
         为 Int，String，Char，返回值类型为 Boolean
      要求调用函数 fun(0, “”, ‘0’)得到返回值为 false，其它情况均返回 true。
  */
    val func = (a: Int, b: String, c: Char) => {
      if (a == 0 && b == "" && c == '0') false else true
    }

    println(s" 匿名函数 ：${func(1, "", '0')} ")
  }

}
