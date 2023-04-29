package com.spark_sql.day01_SparkSql

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

/**
 * Created with IntelliJ IDEA.
 * Description:bigdata
 *
 * @Author: sky
 *          spark sql 的简单 用法
 *          RDD 和  DataFrame 和 DataSet  相互转换
 */
object Day03_1_DF_DS {
  def main(args: Array[String]): Unit = {
    // 创建 环境
    val sparkConf: SparkConf = new SparkConf().setAppName("sparksql").setMaster("local[*]")
    // 从sparkSession 构造中 获取 上下文对象，传入配置
    val spark: SparkSession = SparkSession.builder().config(sparkConf).getOrCreate()

    // 读取 json 文件，得到 DataFrame
    val df: DataFrame = spark.read.json("09_spark/src/main/resources/test/sparkSql_user.json")
    println("======= sql 的 语法 操作 数据 ============")
    // 展示出数据
    //df.show()
    // 创建 临时表 的 视图,使用sql 的形式 操作数据，必须要创建视图，DSL 可以不用
    df.createOrReplaceTempView("user")
    // 使用 sql 查询 数据
    spark.sql("select * from user").show()
    spark.sql("select age from user").show()
    spark.sql("select avg(age) from user").show()
    println()

    // DataFrame DSL 的方式操作，就是调API
    println("============ DSL 的形式 操作，就是调 API ==========")
    df.select("age", "userName").show()

    import spark.implicits._ //将 符号 $ ' 的功能 从 上下文对象中引入，隐式转换
    // 通过隐式转换获取上下文对象的符号功能
    df.select($"age" + 1, $"userName").show() // $ 是引用age字段实现运算，所有字段都要使用$符修饰
    df.select('age + 1, 'userName).show() // 此处' 和 $ 符号的功能一致

    /*
    * RDD  DataFrame DataSet   相互转换
    */
    println("========== RDD  DataFrame DataSet   相互转换 =============")
    // 创建 rdd
    val rddData: RDD[(Int, String, Int)] = spark.sparkContext.makeRDD(List((1, "张三", 20), (2, "刘德华", 60), (3, "成龙", 70)))
    /**
     * DataFrame 转换
     * 参数 是  声明 rdd 数据的 结构
     */
    val df2: DataFrame = rddData.toDF("id", "name", "age") // RDD 转 DF
    // DF 转 RDD
    val rdd: RDD[Row] = df2.rdd // DF 转  RDD


    /**
     * DataSet 的 转换
     */
    // 使用 样例类成为 DataSet 的类型
    val ds: Dataset[User] = df2.as[User] // DF 转 DS
    val ds2df: DataFrame = ds.toDF() // DS 转 DF
    val rdd1: RDD[User] = ds.rdd // DS 转 RDD
    // rdd 数据 转 ds，通过遍历将数据封装进对象，再转 ds
    val rdd2ds: Dataset[User] = rddData.map {
      case (id, name, age) => {
        User(id, name, age)
      }
    }.toDS()

    spark.close()
  }

  // 样例类
  case class User(id: Long, name: String, age: Int)
}


