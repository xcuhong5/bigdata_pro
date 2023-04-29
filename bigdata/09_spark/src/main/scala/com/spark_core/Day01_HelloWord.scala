package com.spark_core

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Created with IntelliJ IDEA.
 * Description:bigdata
 *
 * @Author: sky
 *         1. spark  单词统计
 */
object Day01_HelloWord extends App {
  // 1. spark 配置对象， 本地模式，项目名;local[*] 表示 占满 虚拟核，local是单线程
  val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("wordCount")
  // 2. spark 连接对象，将 配置对象 传入 连接中
  val sc: SparkContext = new SparkContext(sparkConf)
  // 3. 读取文件，获取一行一行的额数据
  val lines: RDD[String] = sc.textFile("09_spark/src/main/resources/test/")
  // 4. 平坦化处理
  val words: RDD[String] = lines.flatMap(_.split(" "))
  /** ******  方式1  ************ */
  // 5. 将 单词 进行 分组
  val word_count: RDD[(String, Int)] = words.groupBy(word => word).mapValues(_.size)
  // 6. 数据 采集 到 内存中
  val array: Array[(String, Int)] = word_count.collect()
  for (elem <- array) {
    println(s" 方式1 单词统计 ：${elem}")
  }
  println()

  /** ******* 方式2  ************* */
  // reduceByKey 相同key 进行 聚合操作
  val counts: RDD[(String, Int)] = words.map((_, 1)).reduceByKey(_ + _)
  // 采集结果 到 内存
  val tuples: Array[(String, Int)] = counts.collect()
  for (elem <- tuples) {
    println(s"使用reduceByKey 统计：${elem}")
  }




  // 关闭 spark 连接
  sc.stop()
}
