package com.xcs.day01_basics

/**
 * Created with IntelliJ IDEA.
 * Description:bigdata
 *
 * @Author: sky
 *          6. 运算符
 *          +   正
 *          - 负
 *            +   加
 *          - 减
 *            *   乘
 *            /   除
 *            %   取模
 *            +   字符相加  "a"+"b"
 *
 *          == 比较运算符
 */
object Operator_06 {
  def main(args: Array[String]): Unit = {
    // + 正   -  负
    val zf = 2
    println(s"正：${+zf} 负：${-zf}")

    // 除号 /， 整数相除 只会保留整数
    val chu = 10 / 3
    println(s"整数 / 整数： 10/3=${chu}")

    val chu_double = 10 / 3.0
    println(s"整数/ 小数： 10/3=${chu_double}")
    // formatted("%.2f") 后面保留两位小数，四舍五入（%4.3f 就是前面保留4位不够的空格补齐，后面保留3位，四舍五入）
    println(s"整数/ 小数,四舍五入： 10/3=${chu_double.formatted("%.2f")}")

    // == 比较的是 值 ，  eq()  比较的 引用； 和 java 相反
    val lisa = new String("lisa")
    val lisa2 = "lisa"
    println(s"值比较 lisa == lisa2: ${lisa == lisa2}")
    println(s"引用 比较 lisa equals lisa2: ${lisa.eq(lisa2)}")
  }
}
