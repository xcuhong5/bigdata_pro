package com.spark_core.day06_Cases

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Created with IntelliJ IDEA.
 * Description:bigdata
 *
 * @Author: sky
 *          需求1： 分析出 商品 热门品类
 */
object Day06_1_Hot_Top10_01 extends App {
  // 创建 spark 配置对象
  val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Day06_1_Hot_Top")
  val sc: SparkContext = new SparkContext(sparkConf)

  // 读取 日志数据
  val file_rdd: RDD[String] = sc.textFile("09_spark/src/main/resources/test/user_visit_action.txt")

  /*
  *  1.统计 品类 点击数量： （品类id，点击数量）
   */
  // 过滤出 只要 点击数据
  val hits_rdd: RDD[String] = file_rdd.filter(datas => {
    // 过滤出 属于点击量的数据（如果点击的品类 ID 和产品 ID 为-1，表示数据不是点击数据）
    val line = datas.split("_")
    line(6) != "-1" // 过滤出 不是-1 的数据,也就是 只要 点击 数据
  })
  // 返回 (品类id，1) 的 格式
  val hitsData_rdd: RDD[(String, Int)] = hits_rdd.map(data => {
    val line: Array[String] = data.split("_") // 根据 — 分割
    val category_id: String = line(6) // 获取 品类 id
    (category_id, 1) // 返回 (品类id，1) 的 格式
  })
  // 进行聚合 统计，统计 品类的 点击量 (品类id，点击量)
  val hitsCount_rdd: RDD[(String, Int)] = hitsData_rdd.reduceByKey(_ + _)

  /*
  * 2.统计 品类 下单数量： （品类id，下单数量）
  */
  // 过滤出 只要 下单数据，下单数据品类id不为null 即可
  val order_rdd: RDD[String] = file_rdd.filter(datas => {
    val line: Array[String] = datas.split("_")
    val order: String = line(8) // 获取 下单数据 的品类id, 此时 品类id 是多个组合的 (1,7,8)这种，下面进行扁平化操作
    order != "null"
  })
  // 将 过滤出 的 下单 品类ids 进行扁平化, 然后进行 据合统计
  val orderData_rdd: RDD[(String, Int)] = order_rdd.flatMap(datas => {
    val line: Array[String] = datas.split("_")
    val ids: Array[String] = line(8).split(",")
    ids.map(id => (id, 1))
  }).reduceByKey(_ + _)


  /*
  * 3.统计 品类 支付数量： （品类id，支付数量）
  */
  val pay_rdd: RDD[String] = file_rdd.filter(datas => {
    val line: Array[String] = datas.split("_")
    val order: String = line(10) // 获取 支付数据 的品类id, 此时 品类id 是多个组合的 (1,7,8)这种，下面进行扁平化操作
    order != "null"
  })
  // 将 过滤出 的 支付数据 品类ids 进行扁平化, 然后进行 据合统计
  val payData_rdd: RDD[(String, Int)] = pay_rdd.flatMap(datas => {
    val line: Array[String] = datas.split("_")
    val ids: Array[String] = line(10).split(",")
    ids.map(id => (id, 1))
  }).reduceByKey(_ + _)

  /*
  * 4.将品类 进行排序，取前10
  * 按照 点击量，订单量，支付量 三个权重排序
  * 形成的数据类型： （品类id，（点击量，订单量，支付量））
   */
  val datas_rdd: RDD[(String, (Iterable[Int], Iterable[Int], Iterable[Int]))] =
    hitsCount_rdd.cogroup(orderData_rdd, payData_rdd)

  // 将 结果数据中的迭代器 取出，因为迭代器中也只有一个值，装进元祖方便排序
  val result_rdd: RDD[(String, (Int, Int, Int))] = datas_rdd.mapValues {
    case (hitsIter, orderIter, payIter) => {
      // 获取 3个 迭代器 中的 元素
      var hitsCount = 0
      val hits_iterator = hitsIter.iterator
      if (hits_iterator.hasNext) {
        hitsCount = hits_iterator.next()
      }

      var orderCount = 0
      val order_iterator = orderIter.iterator
      if (order_iterator.hasNext) {
        orderCount = order_iterator.next()
      }

      var payCount = 0
      val pay_iterator = payIter.iterator
      if (pay_iterator.hasNext) {
        payCount = pay_iterator.next()
      }
      // 返回 从迭代器中获取的 元素
      (hitsCount, orderCount, payCount)
    }
  }

  // 5. 将结果数据 进行排序，元祖的 排序规则是按照 第一个元素 第二个元素依次权重排序的
  val top10_sort_rdd: Array[(String, (Int, Int, Int))] =
    result_rdd.sortBy(_._2, false).take(10)

  // 6. 将结果 输出 控制台
  top10_sort_rdd.foreach(data => println("热门品类Top10：" + data))

  sc.stop()
}
