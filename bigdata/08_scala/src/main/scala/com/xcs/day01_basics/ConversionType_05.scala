package com.xcs.day01_basics

/**
 * Created with IntelliJ IDEA.
 * Description:bigdata
 *
 * @Author: sky
 *          5. 类型转换
 */
object ConversionType_05 {
  def main(args: Array[String]): Unit = {
    /*
    低精度 自动 转高精度
    高精度 转 低精度 需要 强转，调用toxx对象函数
    基本类型 转string，+""
    string 转 基本数据类型，调toxx 函数
    */
    var outo = 1 + 2.0 //自动转成了double
    var down = 2.1.toInt // 调用对象函数 强转
    var str = 3 + "" // 基本数据类型 转string
    var type1 = (4 + "").toInt // string 转 基本数据类型
  }
}
