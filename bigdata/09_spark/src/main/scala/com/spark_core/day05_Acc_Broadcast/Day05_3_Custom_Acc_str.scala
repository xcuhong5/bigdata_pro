package com.spark_core.day05_Acc_Broadcast

import org.apache.spark.rdd.RDD
import org.apache.spark.util.{AccumulatorV2, CollectionAccumulator, DoubleAccumulator, LongAccumulator}
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Created with IntelliJ IDEA.
 * Description:bigdata
 *
 * @Author: sky
 *          自定义累加器 实现 字符串 拼接
 */
object Day05_3_Custom_Acc_str extends App {
  // 创建 spark 配置
  val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("str")
  // 创建 sprak 对象
  val sc: SparkContext = new SparkContext(sparkConf)
  // 准备数据
  val str_rdd: RDD[String] = sc.makeRDD(List("a", "b", "c"))

  // 创建 自定义 累加器 对象
  val acc_Str = new Custom_Acc_Str
  // 向 spark 注册 自定义 累加器
  sc.register(acc_Str, "str")

  str_rdd.foreach(str => {
    println(str)
    acc_Str.add(str)
  })
  println("自定义 累加器 字符串拼接：" + acc_Str.value)

  sc.stop()


  // 自定义 累加器 ，实现
  class Custom_Acc_Str extends AccumulatorV2[String, String] {
    // 定义 字符 串 结果
    var result = ""

    // 判断是否 初始状态
    override def isZero: Boolean = {
      result == ""
    }

    // 累加器 复制
    override def copy(): AccumulatorV2[String, String] = {
      val acc_Str1 = new Custom_Acc_Str()
      acc_Str1.result = this.result
      acc_Str1
    }

    // 重置
    override def reset(): Unit = {
      result = ""
    }

    // 字符串 拼接
    override def add(v: String): Unit = {
      if (null != v) {
        if (isZero) result = v
        else result += "|" + v
      }
    }

    // 合并 分区之间的 数据
    override def merge(other: AccumulatorV2[String, String]) = other match {
      // 获取 其他分区的 字符
      case other_obj: Custom_Acc_Str => {
        val other_str: String = other_obj.value
        if (isZero) result = other_str
        else result += "|" + other_str
      }
    }


    override def value: String = {
      result
    }
  }


}
