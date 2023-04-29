package com.spark_sql.day04_SparkSql_Proj

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
 * Created with IntelliJ IDEA.
 * Description:bigdata
 *
 * @Author: sky
 *          spark sql - 项目： 此处 建表 加载数据的 功能
 *          将 hive-site.xml core-site.xml hdfs-site.xml 复制到 resources/
 *
 */
object Day01_1_Proj_createTable_LoadData {

  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME", "xc")
    // 创建 配置文件
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("proj")
    // sparkSession 的构造获取 上下文对象，支持hive，传入 配置
    val spark = SparkSession.builder().config(sparkConf).enableHiveSupport().getOrCreate()

    /**
     * 创建 表，加载 数据
     */
    spark.sql("""use sparkdata""".stripMargin)
    spark.sql(
      """ CREATE TABLE `user_visit_action`(
        |`date` string,
        |  `user_id` bigint,
        |  `session_id` string,
        |  `page_id` bigint,
        |  `action_time` string,
        |  `search_keyword` string,
        |  `click_category_id` bigint,
        |  `click_product_id` bigint,
        |  `order_category_ids` string,
        |  `order_product_ids` string,
        |  `pay_category_ids` string,
        |  `pay_product_ids` string,
        |  `city_id` bigint)
        |row format delimited fields terminated by '\t' """.stripMargin)
    spark.sql(
      """load data local inpath '09_spark/src/main/resources/test/spark_sql/user_visit_action.txt'
        |into table sparkdata.user_visit_action""".stripMargin)

    spark.sql(
      """CREATE TABLE `product_info`(
        |  `product_id` bigint,
        |  `product_name` string,
        |  `extend_info` string)
        | row format delimited fields terminated by '\t'""".stripMargin)
    spark.sql(
      """ load data local inpath '09_spark/src/main/resources/test/spark_sql/product_info.txt'
        |into table sparkdata.product_info; """.stripMargin)

    spark.sql(
      """CREATE TABLE `city_info`(
        |  `city_id` bigint,
        |  `city_name` string,
        |  `area` string)
        |row format delimited fields terminated by '\t'""".stripMargin)

    spark.sql(
      """load data local inpath '09_spark/src/main/resources/test/spark_sql/city_info.txt'
        |into table sparkdata.city_info""".stripMargin)

    spark.close()
  }
}
