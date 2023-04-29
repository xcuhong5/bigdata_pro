package com.spark_core.day01_createRDD

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Created with IntelliJ IDEA.
 * Description:bigdata
 *
 * @Author: sky
 *          2. textFile
 *          wholeTextFiles 获取文件 来源
 *            文件 中 创建 rdd；读取文件 是行为单位
 *          读取 hdfs 文件 ，将 hdfs-site.xml 拷贝到resource/
 */
object Day01_2_File_CreateRDD extends App {
  // 1. 创建 driver ，spark 配置对象, 本地模式，项目名;local[*] 表示 占满 虚拟核，local是单线程
  val spConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("rdd")
  // 2. 创建 spark 连接对象，将 配置对象 传入
  val sc: SparkContext = new SparkContext(spConf)
  // 3. 从文件 中 创建 rdd; 一行一行的数据 读取
  val dir_rdd: RDD[String] = sc.textFile("09_spark/src/main/resources/test/") // 整个目录下所有文件都读取
  val file_rdd: RDD[String] = sc.textFile("09_spark/src/main/resources/test/Day01_2_File_CreateRDD.txt") // 读取指定文件
  val match_rdd: RDD[String] = sc.textFile("09_spark/src/main/resources/test/Day01_2*") // 通配符 匹配 文件
  match_rdd.foreach(matchs => println(s"本地文件：${matchs}"))

  // 读取 hdfs 文件 创建 rdd， 设置3个分区
  val hdfs_rdd: RDD[String] = sc.textFile("hdfs://xccluster/001.txt")
  // 4. 动作函数 触发 执行, 返回结果
  hdfs_rdd.foreach(data => println(s"文件中 创建rdd ： ${data}"))
  println()

  // wholeTextFiles 可以 获取 文件的来源, 结果是元祖(文件路径，文件内容)
  val whole_rdd: RDD[(String, String)] = sc.wholeTextFiles("09_spark/src/main/resources/test/")
  whole_rdd.collect().foreach(whole_data => println(s"获取文件来源：${whole_data._1} 内容：${whole_data._2}"))


  // 关闭 driver ，释放资源
  sc.stop()
}
