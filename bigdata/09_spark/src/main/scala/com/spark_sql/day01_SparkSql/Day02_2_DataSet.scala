package com.spark_sql.day01_SparkSql

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
 * Created with IntelliJ IDEA.
 * Description:bigdata
 *
 * @Author: sky
 *          DataSet 结构 + 类型的 数据，是 DataFrame 的 扩展
 *          用法 和 df 差不多
 */
object Day02_2_DataSet {
  case class People(var name: String, var age: Int)
  def main(args: Array[String]): Unit = {
    // 创建 配置对象
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("ds")
    // 根据 sparksession 构造获取上下文对象，并且传入 配置对象
    val spark: SparkSession = SparkSession.builder().config(sparkConf).getOrCreate()
    import spark.implicits._ // 隐式转换，可以直接使用 createDataset
    val ds_people = spark.createDataset(List(People("张三", 15), People("sky", 18)))

    spark.close()
  }
}
