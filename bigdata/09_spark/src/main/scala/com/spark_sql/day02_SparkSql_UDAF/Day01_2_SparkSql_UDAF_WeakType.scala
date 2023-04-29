package com.spark_sql.day02_SparkSql_UDAF

import org.apache.spark.SparkConf
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types.{DataType, LongType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

/**
 * Created with IntelliJ IDEA.
 * Description:bigdata
 *
 * @Author: sky
 *          spark sql 的 弱类型 UDAF 自定义函数
 *          弱类型 自定义函数 是根据 索引 定位数据
 */
object Day01_2_SparkSql_UDAF_WeakType {
  def main(args: Array[String]): Unit = {
    // 配置信息
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("weak_udf")
    // 通过 SparkSession 的 构造获得 上下文对象，并传入配置
    val spark: SparkSession = SparkSession.builder().config(sparkConf).getOrCreate()
    // 读取文件 , 获取数据
    val df: DataFrame = spark.read.json("09_spark/src/main/resources/test/sparkSql_user.json")
    // 创建临时 表视图
    df.createOrReplaceTempView("user")

    /**
     * 聚合的 udaf自定义函数 场景是 实现 avg求平均
     * 函数名，函数对应的类对象
     */
    spark.udf.register("avgAge", new AvgUdf())
    spark.sql("select avgAge(age) as avgage from user ").show()

    spark.close()

  }

  /**
   * 弱类型 自定义 函数，是 根据索引 来找到 对应的 数据； 强类型 是根据对象 属性定位数据
   * 此处需求：实现一个  年龄求 avg 功能的自定义函数，缓冲区则是 年龄总和/总条数
   * 1. 继承 UserDefinedAggregateFunction 类，重写 方法:spark3.0后不推荐使用，3.0后使用强类型 Aggregator
   */
  class AvgUdf extends UserDefinedAggregateFunction {
    // 输入参数的 参数名 和 类型: 入参是 年龄 age
    override def inputSchema: StructType = {
      StructType(
        Array(
          StructField("age", LongType)
        )
      )
    }

    // 缓冲区的 参数 和 类型： 缓冲区 聚合的需要参数，age的总和 ， 数据总条数
    override def bufferSchema: StructType = {
      StructType(
        Array(
          StructField("total", LongType),
          StructField("count", LongType)
        )
      )
    }

    // 结果输出的 数据类型
    override def dataType: DataType = LongType

    // 数据 稳定性
    override def deterministic: Boolean = true

    // 缓冲区 初始值，上面bufferSchema 写了几个参数，此处根据索引一一对应
    override def initialize(buffer: MutableAggregationBuffer): Unit = {
      buffer.update(0, 0L)
      buffer.update(1, 0L)
    }

    // 根据输入的值，更新缓冲区: 根据索引 更新 total 和 count
    override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
      // 缓冲区 索引0 是 age 的总和，就是缓冲区的 age + 新输入的 age
      buffer.update(0, buffer.getLong(0) + input.getLong(0))
      // 缓冲区 索引 1 是 总条数，就是记录+1
      buffer.update(1, buffer.getLong(1) + 1)
    }

    // 缓冲区 数据 合并，scala 的分区计算原则，不断更新第一个分区，所以此处更新合并 buffer1
    override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
      buffer1.update(0, buffer1.getLong(0) + buffer2.getLong(0))
      buffer1.update(1, buffer1.getLong(1) + buffer2.getLong(1))
    }

    // 计算 实际业务逻辑，此处求平均年龄，逻辑 是  年龄总和total / 数据条数count
    override def evaluate(buffer: Row): Any = {
      buffer.getLong(0) / buffer.getLong(1)
    }
  }
}
