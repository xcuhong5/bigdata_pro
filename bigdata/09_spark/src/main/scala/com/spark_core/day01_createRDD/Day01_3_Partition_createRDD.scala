package com.spark_core.day01_createRDD

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Created with IntelliJ IDEA.
 * Description:bigdata
 *
 * @Author: sky
 *          3. 分区 ，提高并行度
 *          分区数可以不写，有默认值
 */
object Day01_3_Partition_createRDD extends App {
  // 1. 创建 配置对象，设置master，设置app 名
  val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("wholeText")
  // 2. 创建 driver 对象；连接spark
  val sc: SparkContext = new SparkContext(sparkConf)

  // 3. 内存/ 集合 生成rdd  , 设置 3个分区
  val mem_rdd: RDD[Int] = sc.makeRDD(List(1, 2, 5), 5)
  //  输出 到目录， 一个分区对应一个文件
  mem_rdd.saveAsTextFile("09_spark/src/main/resources/test/mem_rdd_out/")

  // 4. 文件生成rdd 的设置分区，是最小分区数，实际的分区数 会根据底层算法 或多或少设置的 值
  val file_rdd: RDD[String] = sc.textFile("09_spark/src/main/resources/test/Day01_2_File_CreateRDD.txt", 3)
  file_rdd.saveAsTextFile("09_spark/src/main/resources/test/file_rdd_out/")


  sc.stop()
}
