package com.spark_sql.day03_SparkSql_loadData

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
 * Created with IntelliJ IDEA.
 * Description:bigdata
 *
 * @Author: sky
 *          spark 读取 hive 数据: 先启动 hive
 *         1. 将 hive-site.xml 拷贝到 spark/conf/
 *            2. 把 Mysql 的驱动 copy 到 spark/jars/目录下
 *            3. 如果访问不到 hdfs，则需要把 core-site.xml 和 hdfs-site.xml 拷贝到 conf/目录下
 *            4. 将 hive-site.xml 拷贝到 idea 工程 的 resources/
 *            5. 重启 spark-shell
 */
object Day01_2_SparkSql_loadData_Hive {

  def main(args: Array[String]): Unit = {
    // 创建 配置对象
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("hive")
    // 使用 sparksession 的构造获取 上下文 对象，enableHiveSupport() 启用hive支持，传入配置信息
    val spark = SparkSession.builder().enableHiveSupport().config(sparkConf).getOrCreate()
    spark.sql("show tables").show()


    // 关闭 spark
    spark.close()
  }
}
