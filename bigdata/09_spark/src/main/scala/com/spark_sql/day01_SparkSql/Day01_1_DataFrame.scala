package com.spark_sql.day01_SparkSql

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * Created with IntelliJ IDEA.
 * Description:bigdata
 *
 * @Author: sky
 *          DataFrame 是有结构的数据，可以理解为 数据是有字段的
 */
object Day01_1_DataFrame {
  case class People(var name: String, var age: Int, var tel: String)

  case class Address(var addr: String, var tel: String)

  def main(args: Array[String]): Unit = {
    // 创建 配置对象
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("df")
    // 通过 sparksession 的构造获取上下文对象，并传入配置 对象
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()
    // 数据准备
    val peoples: List[People] = List(
      People("张三", 15, "13488986285"),
      People("sky", 16, "13693445110"),
      People("张三", 15, "10086")
    )
    val addrs: List[Address] = List(
      Address("成都", "10086"),
      Address("北京", "13488986285"),
      Address("上海", "13693445110")
    )

    /**
     * 数据 可以 read 读取文件，也可以 用createDataFrame;返回 DataFrame
     */
    val df_people: DataFrame = spark.createDataFrame(peoples)

    println(s"DF 的 数据结构字段：${df_people.schema}")
    println(s"显示 DF 的第一行数据：${df_people.head()}")
    println(s"显示 DF 的第一行数据：${df_people.first()}")

    // 创建 临时表，方便 使用sql 的语法 操作
    df_people.createOrReplaceTempView("people")
    println("===   查询 age > 15 的数据   ====")

    spark.sql("select * from people where age > 15").show()
    println("===   根据下标 获取 数据   ====")
    df_people.rdd.map(row => {
      row(0)
    }).foreach(name => println(s"name:${name}"))

    println(" === 获取平均年龄 ===")
    spark.sql("select avg(age) as age from people").show()
    println()

    /** **************join 操作********************** */
    val df_addr: DataFrame = spark.createDataFrame(addrs) // 准备 数据
    df_addr.createOrReplaceTempView("addr")
    println("== join 内连接  ==")

    val result: DataFrame = spark.sql("select p.*,a.addr from people p join addr a on p.tel = a.tel ")
    result.foreach(row => {
      println(row.toString())
    })

    spark.close()
  }
}
