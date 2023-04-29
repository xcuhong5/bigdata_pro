package com.spark_core.day04_cache_checkPoint_customPartition

import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Created with IntelliJ IDEA.
 * Description:bigdata
 *
 * @Author: sky
 *          spark 持久化，用于数据重用 和 计算时间长的 场景
 *          cache : 数据 临时存储在 内存中，速度快，不安全
 *          persist : 持久化到临时文件，有磁盘io，效率不高，运行完成 文件会自动删除
 *          checkpoint ：有磁盘io，会一直保留文件不会自动删除，提高可靠性，配合检查点先cache在chekpoint，无需重头计算，
 *          会切断血缘关系，磁盘持久化的 数据作为数据源从新建立血缘关系
 *
 */
object Day04_1_Cache extends App {
  // 创建 spark 配置对象
  val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("")
  // 创建 spark driver 对象
  val sc: SparkContext = new SparkContext(sparkConf)
  /*
  检查点 Checkpoint 存储路径，生产环境 是 分布式的 一般 hdfs路径
   */
  sc.setCheckpointDir("09_spark/src/main/resources/test/out/CheckpointDir")
  // 实现 简单 的 word count
  val data_rdd: RDD[String] = sc.makeRDD(List("hello sky", "hello tom"))
  val words_rdd: RDD[String] = data_rdd.flatMap(_.split(" "))
  val word_rdd: RDD[(String, Int)] = words_rdd.map((_, 1))

  /*
  缓存 到内存， 底层 就是persist()
   */
  word_rdd.cache()
  /*
  缓存 到磁盘
   */
  word_rdd.persist(StorageLevel.DISK_ONLY)
  /*
  设置检查点， 结果数据 持久化到磁盘
   */
  word_rdd.checkpoint()

  val word_count: RDD[(String, Int)] = word_rdd.reduceByKey(_ + _)
  word_count.collect().foreach(v => println(s"单词计数：${v}"))
  println()

  /*
  此处 word_rdd 可以复用，如果单纯的 复用一个对象 ，底层其实也是走的 全套流程，并没有复用，
  需要 对此处结果 进行持久化 ，达到复用效果，可以 缓存到 内存，磁盘，磁盘+内存
  word_rdd.cache() // 缓存 到内存
  word_rdd.persist(StorageLevel.DISK_ONLY) // 缓存 到磁盘
   */
  val new_data: RDD[(String, Int)] = word_rdd.map(line => {
    (line._1, line._2 + 1)
  })
  val new_count: RDD[(String, Int)] = new_data.reduceByKey(_ + _)
  new_count.collect().foreach(v => println(s"新的数据集：${v}"))


  sc.stop()
}
