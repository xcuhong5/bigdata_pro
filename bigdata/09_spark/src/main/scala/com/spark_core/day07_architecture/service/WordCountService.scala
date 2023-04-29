package com.spark_core.day07_architecture.service

import com.spark_core.day07_architecture.common.TService
import com.spark_core.day07_architecture.dao.WordCountDao
import org.apache.spark.rdd.RDD

/**
 * Created with IntelliJ IDEA.
 * Description:bigdata
 *
 * @Author: sky
 *          架构分层：服务层
 *          继承 特质，实现混入，重写 抽取的 公共方法
 */
class WordCountService extends TService {
  //调用持久层
  private val dao: WordCountDao = new WordCountDao

  // 重写 特质 中抽取 的 数据计算 方法
  override def dataAss() = {

    // 调用 持久层 dao，读取数据
    val file_rdd: RDD[String] = dao.readFile("09_spark/src/main/resources/test/user_visit_action.txt")

    /*
    * 优化1：  数据 只读取1次，直接通过 判断 匹配数据，将数据封装成 (品类id，(点击量,订单量,支付量)) 的 数据结构
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

    /*
     * 元祖的排序规则 ： 先比较第一个字段，相等再比较第二个字段，第二个相等再比较第三个字段
     * 这个规则符合 当前逻辑，所以 将排序数据 都放在 元祖中
     */
    val top10_sort_rdd: Array[(String, (Int, Int, Int))] =
      datas_rdd.sortBy(_._2, false).take(10)
    top10_sort_rdd
  }
}
