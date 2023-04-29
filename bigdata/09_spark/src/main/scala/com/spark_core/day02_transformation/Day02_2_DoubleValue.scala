package com.spark_core.day02_transformation

import com.spark_core.day02_transformation.Day02_3_KeyValue.sc
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Created with IntelliJ IDEA.
 * Description:bigdata
 *
 * @Author: sky
 *          5. RDD算子 - 转换函数，旧 rdd 逻辑封装成 新的 rdd --双值 的转换函数（ 单数据源操作是单值，多数据源操作是双值，key-value操作）
 *          intersection 交集 ； 要求数据类型一致
 *          union 并集 ； 要求数据类型一致
 *          subtract 差集；要求数据类型一致
 *          zip 拉链  ；要求 分区数 ，分区元素个数一致，类型可以不同
 */
object Day02_2_DoubleValue extends App {
  // 创建 spark 配置对象 ，设置master和 app name
  val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("double")
  // 创建 spark driver 对象
  val sc: SparkContext = new SparkContext(sparkConf)

  // 创建 rdd,  两个数据源进行 双值操作： 要求 数据类型一致
  val source01: RDD[Int] = sc.makeRDD(List(1, 3, 5, 7))
  val source02: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4))
  //val source03: RDD[Int] = sc.makeRDD(List('a', 'b', 1, 3))

  // rdd 的 交集  intersection ： 要求 数据类型一致
  val intersection_rdd: RDD[Int] = source01.intersection(source02)
  intersection_rdd.collect().foreach(v => println(s"rdd 交集 intersection 结果：${v}"))
  println()

  // 并集，就是合并  union ： 要求 数据类型一致
  val union_rdd: RDD[Int] = source01.union(source02)
  union_rdd.collect().foreach(v => println(s"rdd 并集/合并 union 结果：${v}"))
  println()

  // 差集，subtract ，主数据源作为参照，去除两个数据源相同数据，主数据源 剩下的就是差集 ： 要求 数据类型一致
  val sub_rdd: RDD[Int] = source01.subtract(source02)
  sub_rdd.collect().foreach(v => println(s"rdd 差集 subtract 结果：${v}"))
  println()

  /* 拉链 zip，两个数据源 中的元素 根据分区索引顺序 进行配对，数元祖
   要求 两个数据源 分区数相同，分区中的元素相同，数据类型 可以不一致 */
  val zip_rdd: RDD[(Int, Int)] = source01.zip(source02)
  zip_rdd.collect().foreach(v => println(s"rdd 拉链 zip 结果：${v}"))
  println()

  sc.stop()
}
