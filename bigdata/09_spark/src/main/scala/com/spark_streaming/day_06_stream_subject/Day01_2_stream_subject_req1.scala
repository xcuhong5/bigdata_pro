package com.spark_streaming.day_06_stream_subject

import com.spark_streaming.day_06_stream_subject.util.JDBCUtil
import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord}
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}

import java.sql.{Connection, PreparedStatement, ResultSet}
import java.text.SimpleDateFormat
import java.util.Date
import scala.collection.mutable.ListBuffer

/**
 * Created with IntelliJ IDEA.
 * Description:bigdata
 *
 * @Author: sky
 *          需求1：
 *          从kafka 消费数据 主题是 spark_streaming001
 *          1）	读取 Kafka 数据之后，并对 MySQL 中存储的黑名单数据做校验；
 *          2）	校验通过则对给用户点击广告次数累加一并存入 MySQL；
 *          3）	在存入 MySQL 之后对数据做校验，如果单日超过 100 次则将该用户加入黑名单。
 */
object Day01_2_stream_subject_req1 {
  def main(args: Array[String]): Unit = {
    // 创建 环境 配置对象
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("kafka")
    // 创建 上下文 采集器 对象
    val ssc: StreamingContext = new StreamingContext(conf, Seconds(3))

    // 定义Kafka参数
    val kafkaPara: Map[String, Object] = Map[String, Object](
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> "hdp180:9092",
      ConsumerConfig.GROUP_ID_CONFIG -> "xc_req1",
      "key.deserializer" -> "org.apache.kafka.common.serialization.StringDeserializer",
      "value.deserializer" -> "org.apache.kafka.common.serialization.StringDeserializer"
    )

    // 使用 kafka 工具类创建消费者，泛型 是指 kafka 数据的 kv类型
    val kafkaDataDS: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream[String, String](
      ssc, // 上下文 采集对象
      LocationStrategies.PreferConsistent, // 位置策略，采集数据 和 计算数据的节点分配策略，此处失败交给框架自动选择
      // 消费者 策略，数据泛型，参数是 主题topic 和 kafka参数,主题 先创建
      ConsumerStrategies.Subscribe[String, String](Set("spark_streaming001"), kafkaPara)
    )
    // 从消费者 对象 获取 采集周期的 数据，封装 成 对象
    val adClickData_ds: DStream[AdClickData] = kafkaDataDS.map(kafkaData => {
      val data: String = kafkaData.value()
      val datas: Array[String] = data.split(" ")
      AdClickData(datas(0), datas(1), datas(2), datas(3), datas(4))
    })
    /**
     * 遍历 kafka 的数据
     * 读取mysql 的用户黑名单
     * 判断 消费的数据是否存在黑名单用户，对其过滤
     * 统计 过滤后的数据，以天为单位，用户点击广告的次数
     * 当前的 数据 一次就是一个采集周期
     */
    val day_count_ds: DStream[((String, String, String), Int)] = adClickData_ds.transform(rdd => {
      /**
       * TODO 周期性 获取 用户黑名单
       */
      val black_list: ListBuffer[String] = ListBuffer[String]() // 装 黑明单用户的集合
      val mysql_con: Connection = JDBCUtil.getConnection // 获取 mysql连接
      val ps: PreparedStatement = mysql_con.prepareStatement( // sql
        "select userid from sparkStreaming_black_list ")
      val rs: ResultSet = ps.executeQuery() // 执行 sql 获取结果
      while (rs.next()) { // 遍历黑名单结果，添加 到 black_list
        val userid: String = rs.getString(1)
        black_list.append(userid)
      }
      rs.close() // 关闭资源
      ps.close() // 关闭资源
      mysql_con.close() // 关闭连接 避免连接池资源耗尽

      /**
       * TODO 判断用户 是否 在 黑名单
       * 过滤掉黑名单数据，只保留非白名单数据
       */
      val filterRdd: RDD[AdClickData] = rdd.filter(data => {
        // 判断 采集周期的 数据 中 是否存在 黑名单。所以此处取反
        !black_list.contains(data.userId)
      })

      /**
       * TODO 如果用户 不在 黑名单，则进行统计（每个采集周期）
       * 转换 数据的 格式.（（年月日，user，ad），1）
       * 因为 以每天为统计单位，所以 将 day user ad 作为key，统计每天的数量
       */
      val day_count_rdd: RDD[((String, String, String), Int)] = filterRdd.map(data => {
        val sdf: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd")
        val day: String = sdf.format(new Date(data.ts.toLong))
        val userid: String = data.userId
        val ad: String = data.ad
        ((day, userid, ad), 1)
      }).reduceByKey(_ + _)
      day_count_rdd
    })

    /**
     * 对 当前采集周期的 统计结果 - 用户点击广告次数的  数据进行阈值判断
     * 每天 同一个用户对同一个广告点击 > 30 次，判断为恶意点击，加入黑蛋
     */
    day_count_ds.foreachRDD(rdd => {
      rdd.foreachPartition(iter => {
        /**
         * 连接 无法被序列化，也不能 在 driver端，所以使用遍历分区的方式
         * 将连接建立在分区遍历下，优化方案
         */
        val con: Connection = JDBCUtil.getConnection
        iter.foreach {
          case ((day, userid, ad), count) => {
            println(s"${day} ${userid} ${ad} ${count}")
            if (count >= 30) {
              /**
               * TODO 如果 统计结果 超过 阈值，将用户 拉入黑名单
               * 当前 采集周期 统计中 如果用户点击次数 >=30 则加入黑名单
               */
              // 此sql 的意思时，插入时，如果 key 相同，则是执行更新某个字段。此处则是更新 userid
              val sql01: String =
              """
                |insert into sparkStreaming_black_list(userid)
                |VALUES(?)
                |on DUPLICATE key UPDATE userid = ? """.stripMargin
              // 调用封装的 方法。执行SQL语句,单条数据插入
              JDBCUtil.executeUpdate(con, sql01, Array(userid, userid))
            } else {
              /**
               * TODO 如果 统计结果没有 超过阈值 30，则更新 当天的 广告点击数量
               * 先从 数据库查，如果存在 则是更新，不存在则是新增
               */
              // 先查询 该日期 该用户 该广告 的数据 是否存在
              val sql02: String =
                """
                  |select * from sparkStreaming_user_ad_count
                  |where dt = ? and userid = ? and adid = ? """.stripMargin
              // 调用封装的方法。查询数据 是否存在
              val isExist: Boolean = JDBCUtil.isExist(con, sql02, Array(day, userid, ad))
              if (isExist) { // 判断 是否存在
                // 存在 是更新
                val sql03: String =
                  """
                    |update sparkStreaming_user_ad_count set count = count + ?
                    |where dt = ? and userid = ? and adid = ?
                    |""".stripMargin
                // 调用封装，执行 插入
                JDBCUtil.executeUpdate(con, sql03, Array(count, day, userid, ad))

                //  TODO 判断更新后的 广告点击数量  是否超过阈值，超过则将用户 拉入黑名单
                val sql04 =
                  """
                    |select * from sparkStreaming_user_ad_count
                    |where dt = ? and userid = ? and adid = ? and count >= 30
                    |""".stripMargin
                // 调用方法，判断 数据是否存在
                val flag: Boolean = JDBCUtil.isExist(con, sql04, Array(day, userid, ad))

                if (flag) { // 更新后的数据 如果 存在 >= 30 则更新 黑名单
                  val sql05 =
                    """
                      |insert into sparkStreaming_black_list(userid)
                      |VALUES(?)
                      |on DUPLICATE key
                      |UPDATE userid = ?
                      |""".stripMargin
                  JDBCUtil.executeUpdate(con, sql05, Array(userid, userid))
                }

              } else {
                // 不存在，则新增
                val sql05 =
                  """
                    |insert into sparkStreaming_user_ad_count(dt,userid,adid,count)
                    |VALUES(?,?,?,?)
                    |""".stripMargin
                // 调用 插入 的封装 方法
                JDBCUtil.executeUpdate(con, sql05, Array(day, userid, ad, count))
              }
            }
          }
        }
        con.close()
      })
    })
    // 启动 采集器
    ssc.start()
    // 等待关闭 采集器
    ssc.awaitTermination()
  }

  case class AdClickData(ts: String, area: String, city: String, userId: String, ad: String) // 数据封装对象
}
