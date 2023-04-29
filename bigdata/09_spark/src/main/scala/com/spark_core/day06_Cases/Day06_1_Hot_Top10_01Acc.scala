package com.spark_core.day06_Cases

import org.apache.spark.rdd.RDD
import org.apache.spark.util.AccumulatorV2
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

/**
 * Created with IntelliJ IDEA.
 * Description:bigdata
 *
 * @Author: sky
 *          需求1： 分析出 商品 热门品类
 *          最终优化版： 累加器，没有 shuffer，性能更强
 *
 */
object Day06_1_Hot_Top10_01Acc {

  def main(args: Array[String]): Unit = {
    // 创建 spark 配置对象
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Day06_1_Hot_Top_Acc")
    val sc: SparkContext = new SparkContext(sparkConf)

    // 读取 日志数据
    val file_rdd: RDD[String] = sc.textFile("09_spark/src/main/resources/test/user_visit_action.txt")

    // 创建 自定义累加器对象
    val acc: Custom_acc = new Custom_acc
    sc.register(acc, "acc") // 注册 累加器
    /*
    * 优化1：  数据 只读取1次，直接通过 判断 匹配数据，通过 入参(品类id，品类类型) 实现累加器 实现聚合
    */
    file_rdd.foreach(line => {
      val data: Array[String] = line.split("_")
      if (data(6) != "-1") { // 判断是否是 点击数据
        acc.add((data(6), "hits")) // 调用累加器 （品类id，操作类型）
      } else if (data(8) != "null") { // 判断是否是 下单数据
        val ids: Array[String] = data(8).split(",") // 下单数据的 品类id 是多个逗号隔开的，需要拆分遍历
        ids.foreach(id => {
          acc.add((id, "order")) // 调用累加器
        })
      } else if (data(10) != "null") { //判断是否是 支付数据
        val ids: Array[String] = data(10).split(",") // 和订单数据逻辑相同
        ids.foreach(id => {
          acc.add((id, "pay")) // 调用累加器
        })
      }
    })

    // 获取 累加器 数据
    val accValue: mutable.Map[String, HotCategory] = acc.value
    // 只要 map中的 对象即可
    val acc_result: mutable.Iterable[HotCategory] = accValue.map(_._2)

    // 转成 list 自定排序
    val result_data: List[HotCategory] = acc_result.toList.sortWith((left, right) => {
      if (left.hitsCount > right.hitsCount) {
        true
      } else if (left.hitsCount == right.hitsCount) {
        if (left.orderCount > right.orderCount) {
          true
        } else if (left.orderCount == right.orderCount) {
          left.payCount > right.payCount
        } else {
          false
        }
      } else false
    })

    // 取前10
    val top10: List[HotCategory] = result_data.take(10)

    // 6. 将结果 输出 控制台
    top10.foreach(data => println("优化后的 热门品类Top10：" + data))
    sc.stop()

  }


  /** ************** 外部  定义 封装的类 和 自定义累加器 类*********** */
  // 封装 数据类型，封装成 对象
  case class HotCategory(categoryId: String, var hitsCount: Int, var orderCount: Int, var payCount: Int)

  /*
  * 使用 累加器 实现 业务 最后  的 累加 效果
  * 入参 in : 品类id，行为类型
  * 出参 out：mutable.Map[String,HotCategory]
  */
  class Custom_acc extends AccumulatorV2[(String, String), mutable.Map[String, HotCategory]] {

    var resultMap: mutable.Map[String, HotCategory] = mutable.Map[String, HotCategory]()

    // resultMap 为空时则为初始状态
    override def isZero: Boolean = resultMap.isEmpty

    override def copy(): AccumulatorV2[(String, String), mutable.Map[String, HotCategory]] = {
      val custom_acc = new Custom_acc
      custom_acc.resultMap = this.resultMap
      custom_acc

    }

    override def reset(): Unit = resultMap.clear()

    // 核心代码 ： 累加
    override def add(v: (String, String)): Unit = {
      // 获取 品类id 和 操作类型
      val cid: String = v._1
      val category_type: String = v._2
      // 从 map中 查看是否存在，不存在则为新的，赋予一个默认值
      val category: HotCategory = resultMap.getOrElse(cid, HotCategory(cid, 0, 0, 0))
      // 判断 传入的 品类 类型，修改 相应的 数据
      if (category_type == "hits") {
        category.hitsCount += 1
      } else if (category_type == "order") {
        category.orderCount += 1
      } else if (category_type == "pay") {
        category.payCount += 1
      }
      // 将修改后的 数据 更新到 map
      resultMap.update(cid, category)
    }

    // 核心代码： 合并 数据
    override def merge(other: AccumulatorV2[(String, String), mutable.Map[String, HotCategory]]): Unit = {
      // 获取 当前 的 map  和 需要合并的 map
      val map1: mutable.Map[String, HotCategory] = this.resultMap
      val map2: mutable.Map[String, HotCategory] = other.value
      // 遍历 外面的 map，然后 和 自身的map 合并
      map2.foreach {
        case (cid, hc) => {
          // 在当前map 查找其他传入的数据 是否存在，不存在给默认值
          val category: HotCategory = map1.getOrElse(cid, HotCategory(cid, 0, 0, 0))
          // 以当前map 为基准 和 其他map 数据进行 合并
          category.hitsCount += hc.hitsCount
          category.orderCount += hc.orderCount
          category.payCount += hc.payCount
          // 然后 进行 更新
          map1.update(cid, category)
        }
      }
    }

    override def value: mutable.Map[String, HotCategory] = resultMap
  }

}
