package com.xc.spark_tuning.utils

import com.xc.spark_tuning.bean.{School, Student}
import java.util.Random
import org.apache.spark.SparkConf
import org.apache.spark.sql.{SaveMode, SparkSession}

/**
 * 数据 初始化，
 */
object InitUtil {

  def main(args: Array[String]): Unit = {
    // spark 配置对象
    val sparkConf = new SparkConf().setAppName("InitData")
     // .setMaster("local[*]") //TODO 要打包提交集群执行，注释掉
    val sparkSession: SparkSession = initSparkSession(sparkConf)
    initHiveTable(sparkSession)
    initBucketTable(sparkSession)
    saveData(sparkSession)
  }

  // spark - sql 上下文 连接对象 初始化
  def initSparkSession(sparkConf: SparkConf): SparkSession = {
    System.setProperty("HADOOP_USER_NAME", "xc")
    val sparkSession = SparkSession.builder().config(sparkConf).enableHiveSupport().getOrCreate()
    val ssc = sparkSession.sparkContext
    // TODO 改成自己的地址
    ssc.hadoopConfiguration.set("fs.defaultFS", "hdpcluster")
    sparkSession
  }

  /**
   * hive 数据初始化
   * 读取hdfs 数据；输出，按照分区字段分区；格式化 parquet格式；覆盖；保存到表
   *
   * @param sparkSession
   */
  def initHiveTable(sparkSession: SparkSession): Unit = {
    sparkSession.read.json("/spark_data/coursepay.log")
      .write.partitionBy("dt", "dn")
      .format("parquet")
      .mode(SaveMode.Overwrite)
      .saveAsTable("sparktuning.course_pay")

    sparkSession.read.json("/spark_data/salecourse.log")
      .write.partitionBy("dt", "dn")
      .format("parquet")
      .mode(SaveMode.Overwrite)
      .saveAsTable("sparktuning.sale_course")

    sparkSession.read.json("/spark_data/courseshoppingcart.log")
      .write.partitionBy("dt", "dn")
      .format("parquet")
      .mode(SaveMode.Overwrite)
      .saveAsTable("sparktuning.course_shopping_cart")

  }

  def initBucketTable(sparkSession: SparkSession): Unit = {
    sparkSession.read.json("/spark_data/coursepay.log")
      .write.partitionBy("dt", "dn")
      .format("parquet")
      .bucketBy(5, "orderid")
      .sortBy("orderid")
      .mode(SaveMode.Overwrite)
      .saveAsTable("sparktuning.course_pay_cluster")
    sparkSession.read.json("/spark_data/courseshoppingcart.log")
      .write.partitionBy("dt", "dn")
      .bucketBy(5, "orderid")
      .format("parquet")
      .sortBy("orderid")
      .mode(SaveMode.Overwrite)
      .saveAsTable("sparktuning.course_shopping_cart_cluster")
  }

  def saveData(sparkSession: SparkSession) = {
    import sparkSession.implicits._
    sparkSession.range(1000000).mapPartitions(partitions => {
      val random = new Random()
      partitions.map(item => Student(item, "name" + item, random.nextInt(100), random.nextInt(100)))
    }).write.partitionBy("partition")
      .mode(SaveMode.Append)
      .saveAsTable("sparktuning.test_student")

    sparkSession.range(1000000).mapPartitions(partitions => {
      val random = new Random()
      partitions.map(item => School(item, "school" + item, random.nextInt(100)))
    }).write.partitionBy("partition")
      .mode(SaveMode.Append)
      .saveAsTable("sparktuning.test_school")
  }


}
