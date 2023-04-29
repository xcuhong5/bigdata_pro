package com.spark_core.day05_Acc_Broadcast

import org.apache.spark.rdd.RDD
import org.apache.spark.util.{CollectionAccumulator, DoubleAccumulator, LongAccumulator}
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Created with IntelliJ IDEA.
 * Description:bigdata
 *
 * @Author: sky
 *          累加器： 数据增加 和 相加都是累加
 *          实现原理累加器用来把 Executor 端变量信息聚合到 Driver 端
 *          自带的 累加器只有3个：longAccumulator doubleAccumulator  collectionAccumulator
 *          累加器 一般 放置在 行动算子 中
 *          当driver 的 变量 传到 节点，这样的代码 就会形成闭包，节点可以获取闭包数据，但是driver无法获取闭包数据的结果，
 *          所以如果闭包数据需要在driver获得结果和使用，需要使用到累加器 或者 广播变量
 */
object Day05_1_System_Acc extends App {
  // 创建 配置对象
  val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("acc")
  // 创建 driver 对象
  val sc: SparkContext = new SparkContext(sparkConf)


  // 创建数据
  val acc_rdd: RDD[Int] = sc.makeRDD(List(1, 5, 6))
  // 获取 系统 自带 的累加器，设置一个名字 “sum”
  val longAccumulator: LongAccumulator = sc.longAccumulator("sum")
  // 调用 行动 算子，触发job.执行 累加器
  acc_rdd.foreach(num => {
    longAccumulator.add(num)
  })
  println(s"获取 累加器 longAccumulator 中的 结果：${longAccumulator.value}")



  /* double 累加器 */
  val double_rdd: RDD[Double] = sc.makeRDD(List(1.2, 3.2, 3.3))
  val sum_double: DoubleAccumulator = sc.doubleAccumulator("sum_double")
  double_rdd.foreach(num => {
    sum_double.add(num)
  })
  println(s"获取 累加器 DoubleAccumulator 中的 结果：${sum_double.value}")


  // 集合 累加器
  val collect_rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3))
  val collect_acc: CollectionAccumulator[Int] = sc.collectionAccumulator[Int]("add")
  collect_rdd.foreach(num => {
    collect_acc.add(num)
  })
  println(s"集合累加器：${collect_acc.value}")
  sc.stop()
}
