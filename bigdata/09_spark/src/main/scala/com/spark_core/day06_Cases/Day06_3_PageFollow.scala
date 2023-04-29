package com.spark_core.day06_Cases

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

/**
 * Created with IntelliJ IDEA.
 * Description:bigdata
 *
 * @Author: sky
 *          案例3：页面流转
 */
object Day06_3_PageFollow {
  def main(args: Array[String]): Unit = {
    // 创建 spark 配置对象
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Day06_1_Hot_Top10_02Sesson")
    val sc: SparkContext = new SparkContext(sparkConf)

    // 读取 日志数据
    val file_rdd: RDD[String] = sc.textFile("09_spark/src/main/resources/test/user_visit_action.txt")
    file_rdd.cache()

    // 将数据封装成 对象
    val obj_rdd: RDD[UserVisitAction] = file_rdd.map(line => {
      val datas = line.split("_")
      UserVisitAction(
        datas(0),
        datas(1).toLong,
        datas(2),
        datas(3).toLong,
        datas(4),
        datas(5),
        datas(6).toLong,
        datas(7).toLong,
        datas(8),
        datas(9),
        datas(10),
        datas(11),
        datas(12) toLong
      )
    })

    // 统计 分母，就是支付页的 数量
    val fm: Map[Long, Long] = obj_rdd.map(obj => {
      (obj.page_id, 1L)
    }).reduceByKey(_ + _).collect().toMap

    // 统计 分子 数据，先根据 session 分组
    val session_id_rdd: RDD[(String, Iterable[UserVisitAction])] = obj_rdd.groupBy(_.session_id)

    // 根据 时间 升序排序,然后 获取页码，将session 对应下的 页码进行 拉链操作，转换数据结构，统计 页码
    val pcount_rdd: RDD[(String, List[((Long, Long), Int)])] = session_id_rdd.mapValues(obj_v => {
      // 对同 key 下的 数据进行 排序
      val spry_list: List[UserVisitAction] = obj_v.toList.sortBy(_.action_time)
      // 值保留 页码id，返回 session 对应的 所有的 页码
      val page_id: List[Long] = spry_list.map(_.page_id)
      // 页码的 数据格式 需要具有连续性，1,2,3 转为 1-2，2-3，3-4 这种
      val page_tuples: List[(Long, Long)] = page_id.zip(page_id.tail)
      // 现在的 数据就是 (1,2),(2,3)，统计 连续的 页码，最终得到数据(((1,2),1),((2,3),1)) 这种
      page_tuples.map(t1 => {
        (t1, 1)
      })
    })

    // 将数据 结构转换 为 只要 页码数据，不需要 session了, 将一个大的 list，扁平为单个元素
    val data_rdd: RDD[((Long, Long), Int)] = pcount_rdd.map(_._2).flatMap(list => list)
    // 页码 统计 结果，（（1,2），10）
    val page_sum: RDD[((Long, Long), Int)] = data_rdd.reduceByKey(_ + _)
    // 计算 单跳 转化率
    page_sum.foreach {
      case ((page1, page2), sum) => {
        // 分母
        val l: Long = fm.getOrElse(page1, 0L)
        println(s"页面${page1} 跳转至 ${page2} 的 转换率 是 ${sum.toDouble / l.toDouble}")
      }
    }


    sc.stop()
  }

  // 数据样例类，用户访问动作表
  case class UserVisitAction(
                              date: String, //用户点击行为的日期
                              user_id: Long, //用户的 ID
                              session_id: String, //Session 的 ID
                              page_id: Long, //某个页面的 ID
                              action_time: String, //动作的时间点
                              search_keyword: String, //用户搜索的关键词
                              click_category_id: Long, //某一个商品品类的 ID
                              click_product_id: Long, //某一个商品的 ID
                              order_category_ids: String, //一次订单中所有品类的 ID 集合
                              order_product_ids: String, //一次订单中所有商品的 ID 集合
                              pay_category_ids: String, //一次支付中所有品类的 ID 集合
                              pay_product_ids: String, //一次支付中所有商品的 ID 集合
                              city_id: Long
                            ) //城市 id
}


