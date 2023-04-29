package com.spark_sql.day02_SparkSql_UDAF

import org.apache.spark.SparkConf
import org.apache.spark.sql.expressions.Aggregator
import org.apache.spark.sql.{DataFrame, Encoder, Encoders, SparkSession, functions}

/**
 * Created with IntelliJ IDEA.
 * Description:bigdata
 *
 * @Author: sky
 *          spark sql 的  强类型 udaf 自定义函数
 *          强类型 是通过 对象属性 定位数据
 *          当前 的方法使用是3.0后的，
 *          3.0前的强类型 只需要将Aggregator[Emp, UserBuff, Long]，将输入参数类型改为 类对象
 */
object Day01_3_SparkSql_UDAF_Strongly_Type {
  def main(args: Array[String]): Unit = {
    // 配置信息
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("strongly_udaf")
    // 通过 SparkSession 的 构造获得 上下文对象，并传入配置
    val spark: SparkSession = SparkSession.builder().config(sparkConf).getOrCreate()
    // 读取文件 , 获取数据
    val df: DataFrame = spark.read.json("09_spark/src/main/resources/test/sparkSql_user.json")
    // 创建临时 表视图
    df.createOrReplaceTempView("user")

    /**
     * 聚合的 自定义函数 场景是 实现 avg求平均
     * 函数名，函数对应的类对象
     */
    spark.udf.register("avgAge", functions.udaf(new Avgudaf()))
    spark.sql("select avgAge(age) as avgage from user ").show()
    spark.close()
  }

  case class Emp(age: Long, userName: String) // 3.0 版本前的 强类型 使用，此处只做声明，参考顶部注释

  case class UserBuff(var total: Long, var count: Long) // 强类型，缓冲区 类型

  /**
   * 弱类型 自定义 函数，是 根据索引 来找到 对应的 数据； 强类型 是根据对象 属性定位数据
   * 此处需求：实现一个  年龄求 avg 功能的自定义函数，缓冲区则是 年龄总和/总条数
   * 1. 继承 Aggregate[In,Buff,Out] 泛型, 重写 方法
   * in 输入参数的类型
   * buff 缓冲区聚合的数据类型
   * out 输出的 数据类型
   */
  class Avgudaf extends Aggregator[Long, UserBuff, Long] {
    // 缓冲区 的 初始值
    override def zero: UserBuff = {
      UserBuff(0L, 0L)
    }

    // 根据 输入的 数据 更新缓冲区
    override def reduce(buff: UserBuff, in: Long): UserBuff = {
      buff.total = buff.total + in // 输入参数是 age， 先累加
      buff.count = buff.count + 1 // 统计 数据 条数
      buff // 返回 对象
    }

    // 缓冲区 合并
    override def merge(buff1: UserBuff, buff2: UserBuff): UserBuff = {
      buff1.total = buff1.total + buff2.total
      buff1.count = buff1.count + buff2.count
      buff1
    }

    // 计算结果: age 总数 / 总记录数 = 平均年龄
    override def finish(buff: UserBuff): Long = {
      buff.total / buff.count
    }

    // 缓冲区 编码 ，自定义的类就是 Encoders.product 固定写法；此处UserBuff 是自定义类
    override def bufferEncoder: Encoder[UserBuff] = Encoders.product

    // 输出 数据的 编码操作，这是scala已经存在的类型
    override def outputEncoder: Encoder[Long] = Encoders.scalaLong
  }
}
