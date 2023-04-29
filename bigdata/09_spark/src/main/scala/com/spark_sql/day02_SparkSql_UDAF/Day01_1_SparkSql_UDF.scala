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
 *          spark sql 的 弱类型 UDF 自定义函数
 *          弱类型 自定义函数 是根据 索引 定位数据
 */
object Day01_1_SparkSql_UDF {
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
     * 简单的  udf ，涉及聚合的 udaf 此方法不适用，需要单独写自定义的类
     */
    // 自定义函数 age+1，（函数名字，参数=>函数体）
    spark.udf.register("addNum", (age: Long) => {
      age + 1
    })
    // 自定义函数 userName加前缀，（函数名字，参数=>函数体）
    spark.udf.register("prefix", (userName: String) => {
      "name:" + userName
    })

    // 通过 udf 自定义函数，给age+1，给 name 加前缀
    spark.sql("select addNum(age) as age,prefix(userName) as userName from user").show()
    spark.sql("select age+1 from user").show()

    spark.close()

  }


}
