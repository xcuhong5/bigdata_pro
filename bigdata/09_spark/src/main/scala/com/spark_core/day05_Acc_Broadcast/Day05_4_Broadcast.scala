package com.spark_core.day05_Acc_Broadcast

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

/**
 * Created with IntelliJ IDEA.
 * Description:bigdata
 *
 * @Author: sky
 *          广播变量：
 *          广播变量用来高效分发较大的对象。向所有工作节点发送一个较大的只读值，以供一个或多个 Spark 操作使用
 *          当spark rdd 使用外部数据就会形成闭包，这样每个task 都会有一份外部数据，
 *          会造成数据冗余和内存占用，广播变量则是将数据发送到节点，让spark任务 共享一份数据。
 *          当driver 的 变量 传到 节点，这样的代码 就会形成闭包，节点可以获取闭包数据，但是driver无法获取闭包数据的结果，
 *          所以如果闭包数据需要在driver获得结果和使用，需要使用到累加器 或者 广播变量
 *
 */
object Day05_4_Broadcast extends App {

  // 创建 spark 配置对象
  val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("broadcast")
  // 创建 spark 上下文对象
  val sc: SparkContext = new SparkContext(sparkConf)
  // 准备数据
  val rdd1: RDD[(String, Int)] = sc.makeRDD(List(("a", 1), ("b", 2), ("c", 2)))
  val maps: mutable.Map[String, Int] = mutable.Map(("a", 2), ("b", 3), ("c", 4))
  // 需求是 将 数据 进行 join 实现： ((a,(1,2)),(b,(2,3))) 的效果;将 maps 数据 通过广播发送到集群
  val bc: Broadcast[mutable.Map[String, Int]] = sc.broadcast(maps)
  // 获取 广播 变量的值，进行数据组装
  val rdd_data: RDD[(String, (Int, Int))] = rdd1.map {
    case (w, c) => {
      val v = bc.value.getOrElse(w, 0)
      (w, (c, v))
    }
  }
  rdd_data.foreach(str => println("广播变量测试：" + str))
  sc.stop()
}
