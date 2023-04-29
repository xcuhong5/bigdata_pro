package com.spark_sql.day04_SparkSql_Proj

import org.apache.spark.SparkConf
import org.apache.spark.sql.{Encoder, Encoders, SparkSession, functions}
import org.apache.spark.sql.expressions.Aggregator

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

/**
 * Created with IntelliJ IDEA.
 * Description:bigdata
 *
 * @Author: sky
 *          spark sql - 项目： 此处 是 各区域热门商品 top3,根据点击量为主导，
 *          并备注该商品主要分布城市，超过2个城市用其他代替
 *          将 hive-site.xml core-site.xml hdfs-site.xml 复制到 resources/
 *
 */
object Day01_2_Proj_AreaHotProductTop3 {

  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME", "xc")
    // 创建 配置文件
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("top3")
    // sparkSession 的构造获取 上下文对象，支持hive，传入 配置
    val spark = SparkSession.builder().config(sparkConf).enableHiveSupport().getOrCreate()
    spark.sql("use sparkdata")
    spark.sql(
      """select
        | a.*,p.product_name,c.city_name,c.area
        |	from user_visit_action a
        |	  join product_info p on a.click_product_id = p.product_id
        |	  join city_info c on a.city_id = c.city_id
        |   where a.click_product_id  > -1 """.stripMargin).createOrReplaceTempView("t1")

    spark.udf.register("cityRemark", functions.udaf(new CityRemark()))
    // 根据 区域商品 进行 聚合操作
    spark.sql(
      """select
        |	area,product_name,count(*) as clickCount,
        |  cityRemark(city_name) as city_remark
        |	from t1 group by t1.area,t1.product_name """.stripMargin).createOrReplaceTempView("t2")


    // 区域内 点击数据排行
    spark.sql(
      """select *,
        |		rank() over(partition by area order by clickCount desc) as rank
        |		from t2""".stripMargin).createOrReplaceTempView("t3")

    // 取 区域排行 前三
    spark.sql(""" select * from t3 where rank <= 3 """.stripMargin).show(truncate = false)

    spark.close()
  }

  case class Buffer(var total: Long, var city_map: mutable.Map[String, Long])

  // 自定义 udaf ，实现 区域备注
  class CityRemark extends Aggregator[String, Buffer, String] {
    // 缓冲区 初始值
    override def zero: Buffer = Buffer(0, mutable.Map[String, Long]())

    // 更新缓冲区
    override def reduce(buf: Buffer, city: String): Buffer = {
      buf.total += 1 // 区域总数
      // 统计相同区域数量
      val newCount = buf.city_map.getOrElse(city, 0L) + 1
      buf.city_map.update(city, newCount) // 更新区域信息
      buf
    }

    // 合并数据,scala 底层如果两个集合合并，则是更新第一个
    override def merge(buf1: Buffer, buf2: Buffer): Buffer = {
      buf1.total = buf1.total + buf2.total // 更新 区域总和 total
      val map1: mutable.Map[String, Long] = buf1.city_map // 取出map，方便计算
      val map2: mutable.Map[String, Long] = buf2.city_map
      // foldLeft 集合合并
      val newMap: mutable.Map[String, Long] = map1.foldLeft(map2) {
        case (map, (city, count)) => { // 模式匹配进行 更新
          val newCount = map.getOrElse(city, 0L) + count
          map.update(city, newCount)
          map
        }
      }
      // 更新 buffer 中的 map，返回
      buf1.city_map = newMap
      buf1
    }

    // 计算 逻辑 和  结果
    override def finish(reduction: Buffer): String = {
      val listBuffer = ListBuffer[String]()
      val total = reduction.total
      val city_map = reduction.city_map
      // 对区域数据进行排序
      val city_sort_list: List[(String, Long)] = city_map.toList.sortWith((left, right) => {
        left._2 > right._2
      }).take(2)
      // 判断是否有多的
      val hasMore = city_map.size > 2
      var sumRes = 0L // 前两名比列总和 变量
      city_sort_list.foreach { // 遍历 区域数据
        case (city, count) => {
          val res = count * 100 / total  // 计算比例，*100 是避免小数
          listBuffer.append(s"${city} ${res}%") // 最终 结果数据 装进 list
          sumRes += res // 累加 得到前2名的比例
        }
      }
      // 判断区域 数据 是否>2
      if (hasMore) {
        // 大于2个 的用其他，则是 100-前2名比例总和 = 其他比列
        listBuffer.append(s"其他 ${100 - sumRes}%")
      }
      // 返回结果 ，逗号隔开
      listBuffer.mkString(",")
    }
    // 缓冲数据 编码，自定义类型的 固定写法 Encoders.product
    override def bufferEncoder: Encoder[Buffer] = Encoders.product
    // 输出 编码是 字符串
    override def outputEncoder: Encoder[String] = Encoders.STRING
  }
}

/*
select
*
from (
select
*,
rank() over(partition by area order by clickCount desc) as rank
from (
select
area,
product_name,
count(*) as clickCount
from(
select
a.*,
p.product_name,
c.city_name,
c.area
from
user_visit_action a
join product_info p on a.click_product_id = p.product_id
join city_info c on a.city_id = c.city_id where a.click_product_id  > -1
)t1 group by t1.area,t1.product_name
)t2
)t3 where rank <= 3*/
