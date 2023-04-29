package com.spark_core.day01_createRDD

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Created with IntelliJ IDEA.
 * Description:bigdata
 *
 * @Author: sky
 *         1. 内存中（将集合 生成 rdd） 创建rdd
 *            parallelize()  和  makeRDD(),makeRDD底层其实就是 parallelize
 *
 */
object Day01_1_Mem_CreateRDD extends App {
  // 1. 创建 driver ，spark 配置对象, 本地模式，项目名;local[*] 表示 占满 虚拟核，local是单线程
  val spConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("rdd")
  // 2. 创建 spark 连接对象，将 配置对象 传入
  val sc: SparkContext = new SparkContext(spConf)
  // 3. 从集合中，也叫内存中 创建 rdd
  val list_rdd: RDD[Int] = sc.parallelize(List(1, 3, 5)) // 方法1
  val m_rdd: RDD[Int] = sc.makeRDD(List(5, 7, 9)) // 方法2，底层也是 parallelize
  // 4. 动作函数，触发 执行程序，返回结果
  list_rdd.collect().foreach(tmp => println(s"方法1 执行结果：${tmp}"))
  println()
  m_rdd.foreach(tmp => println(s"方法2 执行结果 ：${tmp}"))
  // 关闭 spark driver ，释放资源
  sc.stop()
}
