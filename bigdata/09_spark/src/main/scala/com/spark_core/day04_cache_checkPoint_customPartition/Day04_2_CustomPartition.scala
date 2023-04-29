package com.spark_core.day04_cache_checkPoint_customPartition

import org.apache.spark.rdd.RDD
import org.apache.spark.{Partitioner, SparkConf, SparkContext}

/**
 * Created with IntelliJ IDEA.
 * Description:bigdata
 *
 * @Author: sky
 *          自定义分区： 根据数据的 key 分配指定分区
 */
object Day04_2_CustomPartition extends App {
  // 创建spark 配置对象
  val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("partition")
  // 创建 spark driver 对象
  val sc: SparkContext = new SparkContext(sparkConf)

  // 根据 key 分配到 指定分区； cba在0分区 nba 在1分区 其他 2分区
  val partition_rdd: RDD[(String, String)] = sc.makeRDD(List(
    ("cba", "广西队进球1-0"),
    ("nba", "霍比特人 1-3"),
    ("mba", "no news"),
    ("cba", "四川队 夺冠")), 3
  )
  val custom_parti: RDD[(String, String)] = partition_rdd.partitionBy(new MyPartition)
  custom_parti.saveAsTextFile("09_spark/src/main/resources/test/out/custom_partition")
  sc.stop()
}

/**
 * 自定义分区 类： 继承 Partitioner ，重写 方法
 */
class MyPartition extends Partitioner {
  // 自定义 分区 数量
  override def numPartitions: Int = 3

  // 分区 规则,返回 分区 索引; cba在0分区 nba 在1分区 其他 2分区
  override def getPartition(key: Any): Int = {
    key match {
      case "cba" => 0
      case "nba" => 1
      case _ => 2
    }
  }
}