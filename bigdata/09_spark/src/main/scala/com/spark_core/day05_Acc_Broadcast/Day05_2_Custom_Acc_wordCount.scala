package com.spark_core.day05_Acc_Broadcast

import org.apache.spark.rdd.RDD
import org.apache.spark.util.AccumulatorV2
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

/**
 * Created with IntelliJ IDEA.
 * Description:bigdata
 *
 * @Author: sky
 *          自定义 累加器, 实现 wordCount
 */
object Day05_2_Custom_Acc_wordCount extends App {
  // 创建 spark 配置对象
  val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("c_acc")
  // 创建 spark driver 对象
  val sc = new SparkContext(sparkConf)
  // 准备数据
  val wc_rdd = sc.makeRDD(List("spark", "scala", "flink", "hadoop", "spark"))


  // 创建 自定义累加器 对象
  val custom_Acc_obj = new Custom_Acc

  // 将 自定义累加器 对象 注册到 spark，设置 名字
  sc.register(custom_Acc_obj, "custom_acc")

  // 动作函数 中 写 累加器
  wc_rdd.foreach(wc => {
    custom_Acc_obj.add(wc)
  })
  println("自定义累加器 单词计数结果：" + custom_Acc_obj.value)
  sc.stop()

  /** 自定义 累加器 ，实现 AccumulatorV2[in,out] 抽象类
   * in 入参
   * out 出参
   * 重写 累加器 方法
   * 自定义累加器 只需要 关注 add 和 merge 方法
   */
  class Custom_Acc extends AccumulatorV2[String, mutable.Map[String, Long]] {
    // 定义一个 存放 单词数据 的 集合
    val wc_map: mutable.Map[String, Long] = mutable.Map[String, Long]()

    // 判断 是否 是 初始 状态
    override def isZero: Boolean = {
      wc_map.isEmpty // 如果 装 单词数据的 集合 是空的 ，就是 初始状态
    }

    // 复制累加器, 在方法中 创建 新的 累加器
    override def copy(): AccumulatorV2[String, mutable.Map[String, Long]] = {
      new Custom_Acc()
    }

    // 重置，将 map 清空重置
    override def reset(): Unit = {
      wc_map.clear() // 清空 存放单词计数的  map
    }

    // 获取 累加器 需要 累加的 值
    override def add(word: String): Unit = {
      /* 需要添加到累加器的 数据进来，
      先从map 中查看 是否存在，不存在则 默认0，计数为0+1，
      存在 则将对应的 计数+1
      */
      val count: Long = wc_map.getOrElse(word, 0L) + 1
      // 更新 map，单词存在则是更新计数，不存在则是新增
      wc_map.update(word, count)
    }

    // 分区间的 other map 和 当前的 map 合并
    override def merge(other: AccumulatorV2[String, mutable.Map[String, Long]]): Unit = {
      // 获取 当前的 map 和   other map
      val this_map: mutable.Map[String, Long] = this.wc_map
      val other_map: mutable.Map[String, Long] = other.value
      // 遍历 其他分区的 单词计数
      other_map.foreach {
        case (words, count) => {
          // 其他分区的 元素 和 当前map 进行合并，有相同的 则 累加计数
          val newWord: Long = this_map.getOrElse(words, 0L) + count
          // 获取 其他分区数据 然后 更新 到 当前 map
          this_map.update(words, newWord)
        }
      }
    }

    // 累加器的 最终结果，直接将 存放 单词计数的 map 返回
    override def value: mutable.Map[String, Long] = wc_map
  }

}
