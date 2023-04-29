package com.spark_core.day03_action_saveAndLoadFile

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Created with IntelliJ IDEA.
 * Description:bigdata
 *
 * @Author: sky
 *          行动 算子，触发程序执行
 *          reduce()  先聚合 分区内 再聚合 分区间数据
 *          collect()  返回 所有 数据
 *          count()  返回 元素的 个数
 *          first()  返回 第一个元素
 *          take() 返回 数据 前n 个 元素，组成 数组
 *          takeOrdered() 返回 数据排序后的 前n 个 元素，组成 数组,此处降序，不写第二个参数就是 正序
 *          aggregate()() 使用初始值 进行分区内计算，再使用初始值 进行分区间计算
 *          fold(0)() 使用初始值 进行 计算，分区内 和 分区间规则一致
 *          countByKey() 统计每种key的个数
 *          foreach()  分布式 打印，顺序无法确定
 *          save & load   文件的 保存 和 读取
 */
object Day03_1_Action_saveOrLoadFile extends App {

  // 创建 spark 配置对象 设置 master 和 name
  val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("action")
  // 创建 spark driver 对象
  val sc: SparkContext = new SparkContext(sparkConf)

  val rdd_val: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4), 2)
  val reduce_action: Int = rdd_val.reduce(_ + _) // 先聚合 分区内 再聚合 分区间数据
  println(s"reduce 返回 数据集的 聚合结果：${reduce_action}")
  println()

  val collect_action: Array[Int] = rdd_val.collect() // 返回 所有 数据
  collect_action.foreach(v => println(s"collect 返回 所有 数据：${v}"))

  println()

  val count_action: Long = rdd_val.count() // 返回 元素的 个数
  println(s"count 返回 元素的 个数：${count_action}")
  println()

  val frist_action: Int = rdd_val.first() // 返回 第一个元素
  println(s"first 返回 第一个元素：${frist_action}")
  println()

  val take_action: Array[Int] = rdd_val.take(2) //返回 数据 前n 个 元素，组成 数组
  take_action.foreach(v => println(s"take 返回 数据 前n 个 元素，组成 数组：${v}"))
  println()

  //返回 数据排序后的 前n 个 元素，组成 数组,此处降序，不写第二个参数就是 正序
  val takeOrdered_action: Array[Int] = rdd_val.takeOrdered(3)(Ordering.Int.reverse)
  takeOrdered_action.foreach(v => println(s"takeOrdered 返回 数据排序后的 前n 个 元素，组成 数组：${v}"))
  println()

  //使用初始值 进行分区内计算，再使用初始值 进行分区间计算, 分区内：10+1+2=13  10+3+4=17 分区间： 10+13+17=40
  val aggregate_action: Int = rdd_val.aggregate(10)(_ + _, _ + _)
  println(s"aggregate 使用初始值 进行分区内计算，再使用初始值 进行分区间计算：${aggregate_action}")
  println()

  val fold_action: Int = rdd_val.fold(0)(_ + _) // 使用初始值 进行 计算，分区内 和 分区间规则一致
  println(s"fold 使用初始值 进行 计算，分区内 和 分区间规则一致：${fold_action}")
  println()

  val rdd: RDD[(Int, String)] = sc.makeRDD(List((1, "a"), (1, "a"), (1, "a"), (2, "b"), (3, "c"), (3, "c")), 2)
  val result: collection.Map[Int, Long] = rdd.countByKey() // 统计每种key的个数
  println(s"countByKey 统计每种key的个数：${result}")
  println()

  // 这个是 分布式 打印，是在excutor 执行的 遍历
  rdd.foreach(println)
  // 这个 foreach 是 driver 的 循环打印
  rdd.collect().foreach(println)


  /** *** 文件的  保存  和 读取  *** */
  // 保存成Text文件
  rdd.saveAsTextFile("09_spark/src/main/resources/test/out/saveAsTextFile")
  // 序列化成对象保存到文件
  rdd.saveAsObjectFile("09_spark/src/main/resources/test/out/saveAsObjectFile")
  // 保存成 Sequencefile 文件,要求 数据 必须 为kv 类型
  rdd.saveAsSequenceFile("09_spark/src/main/resources/test/out/saveAsSequenceFile")

  // 读取 文本 文件
  val textFile_rdd: RDD[String] = sc.textFile("09_spark/src/main/resources/test/out/saveAsTextFile")
  println("读取 textFile ： " + textFile_rdd.collect().mkString(","))
  println()

  // 读取 序列化对象文件， 数据 的泛型 kv 类型
  val object_file_rdd: RDD[(Int, String)] =
    sc.objectFile[(Int, String)]("09_spark/src/main/resources/test/out/saveAsObjectFile")
  println("读取 objectFile ： " + object_file_rdd.collect().mkString(","))
  println()

  // 读取 二进制 kv 型 文件Sequencefile
  val sequenceFile_rdd: RDD[(Int, String)] =
    sc.sequenceFile[Int, String]("09_spark/src/main/resources/test/out/saveAsSequenceFile")
  println("读取 sequenceFile ：" + sequenceFile_rdd.collect().mkString(","))

  sc.stop()
}
