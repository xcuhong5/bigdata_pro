package com.spark_core.day06_Cases

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD


/**
 * Created with IntelliJ IDEA.
 * Description:bigdata
 *
 * @Author: sky
 *         案列2： 统计出 top10 热门品类 ,每个品类 对应的 top10 session
 *          (品类id，（session，总数）)
 */
object Day06_2_Hot_Top10_Sesson {
  def main(args: Array[String]): Unit = {

    // 创建 spark 配置对象
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Day06_1_Hot_Top10_02Sesson")
    val sc: SparkContext = new SparkContext(sparkConf)

    // 读取 日志数据
    val file_rdd: RDD[String] = sc.textFile("09_spark/src/main/resources/test/user_visit_action.txt")
    file_rdd.cache()

    /*
    * 获取 热门品类 数据
    * 数据 只读取1次，直接通过 判断 匹配数据，将数据封装成 (品类id，(点击量,订单量,支付量)) 的 数据结构
    */
    val data_rdd: RDD[(String, (Int, Int, Int))] = file_rdd.flatMap(line => {
      val data: Array[String] = line.split("_")
      if (data(6) != "-1") { // 判断是否是 点击数据
        List((data(6), (1, 0, 0))) // 将数据 封装成 (品类id，(点击量,0,0))
      } else if (data(8) != "null") { // 判断是否是 下单数据
        val ids: Array[String] = data(8).split(",") // 下单数据的 品类id 是多个逗号隔开的，需要拆分遍历
        ids.map(id => {
          (id, (0, 1, 0)) // 遍历 订单 id，封装 数据结构  (品类id，(0,订单量,0))
        })
      } else if (data(10) != "null") { //判断是否是 支付数据
        val ids: Array[String] = data(10).split(",") // 和订单数据逻辑相同
        ids.map(id => {
          (id, (0, 0, 1))
        })
      } else Nil
    })

    // 然后 进行聚合，相同的  key，两两相加
    val datas_rdd: RDD[(String, (Int, Int, Int))] = data_rdd.reduceByKey {
      case (data1, data2) => {
        (data1._1 + data2._1, data1._2 + data2._2, data1._3 + data2._3)
      }
    }

    // 获取 top10 热门品类的 id
    val top10_ids: Array[String] = datas_rdd.sortBy(_._2, false).take(10).map(_._1)

    // 过滤数据 筛选出 热门品类top10的 session
    val hotData_rdd: RDD[String] = file_rdd.filter(line => {
      val datas: Array[String] = line.split("_")
      if (datas(6) != "-1") {
        top10_ids.contains(datas(6))
      } else false
    })

    // 分析 过滤出的品类数据，封装 数据 ((品类id，session)，计数)，然后 根据key相同聚合
    val addResult_rdd: RDD[((String, String), Int)] = hotData_rdd.map(line => {
      val datas: Array[String] = line.split("_")
      ((datas(6), datas(2)), 1)
    }).reduceByKey(_ + _)

    // 重新 写 数据结构 (品类id，(session，计数总数))，然后 根据 key 分组
    val result_rdd: RDD[(String, Iterable[(String, Int)])] = addResult_rdd.map {
      case ((cid, sid), sum) => {
        (cid, (sid, sum))
      }
    }.groupByKey()

    // 排序， 返回 top10品类 里面的 top10 session
    val sessionTop10: RDD[(String, List[(String, Int)])] = result_rdd.mapValues(v => {
      v.toList.sortBy(_._2)(Ordering.Int.reverse).take(10)
    })
    // 输出 控制台
    sessionTop10.foreach(r => println("top10 品类中的  top10 session：" + r))
    sc.stop()
  }
}
