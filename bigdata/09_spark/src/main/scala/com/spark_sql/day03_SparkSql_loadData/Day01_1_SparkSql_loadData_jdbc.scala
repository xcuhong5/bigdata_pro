package com.spark_sql.day03_SparkSql_loadData

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

/**
 * Created with IntelliJ IDEA.
 * Description:bigdata
 *
 * @Author: sky
 *          spark sql 读取 mysql 数据
 *          引入 mysql 驱动依赖
 */
object Day01_1_SparkSql_loadData_jdbc {
  def main(args: Array[String]): Unit = {
    // 创建配置对象
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("loadMysql")
    // 通过 spark session 构造 获取 上下文对象
    val spark: SparkSession = SparkSession.builder().config(sparkConf).getOrCreate()

    import spark.implicits._

    //方式1：通用的load方法读取 spark.read.format("jdbc")
    val df: DataFrame = spark.read.format("jdbc")
      .option("url", "jdbc:mysql://hdp180:3306/sparkDB")
      .option("driver", "com.mysql.jdbc.Driver")
      .option("user", "root")
      .option("password", "root")
      .option("dbtable", "user")
      .load()
    df.show()

    df.write.format("jdbc")
      .option("url", "jdbc:mysql://hdp180:3306/sparkDB")
      .option("driver", "com.mysql.jdbc.Driver")
      .option("user", "root")
      .option("password", "root")
      .option("dbtable", "user2")
      .mode(SaveMode.Append) // 保存模式是 追加，就是表不存在则新建
      .save()


    spark.close()
  }
}
