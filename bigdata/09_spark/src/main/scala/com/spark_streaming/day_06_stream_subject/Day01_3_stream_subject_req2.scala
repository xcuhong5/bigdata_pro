package com.spark_streaming.day_06_stream_subject
import com.spark_streaming.day_06_stream_subject.util.JDBCUtil
import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import java.sql.{Connection}
import java.text.SimpleDateFormat
import java.util.Date


/**
 * Created with IntelliJ IDEA.
 * Description:bigdata
 *
 * @Author: sky
 *          需求2：
 *          从kafka 消费数据 主题是 spark_streaming001
 *          1）	单个批次内对数据进行按照天维度的聚合统计
 *          2）	结合 MySQL 数据跟当前批次数据更新原有的数据。
 */
object Day01_3_stream_subject_req2 {
  def main(args: Array[String]): Unit = {
    // 创建 环境 配置对象
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("req2")
    // 创建 上下文 采集器 对象
    val ssc: StreamingContext = new StreamingContext(conf, Seconds(3))

    // 定义Kafka参数
    val kafkaPara: Map[String, Object] = Map[String, Object](
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> "hdp180:9092,hdp181:9092,hdp182:9092",
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
     * 统计 当前 采集周期的 数据。日期 区域 城市 广告 为单位 统计数量
     * ((day,area,city,ad),1)
     */
    val area_count_ds: DStream[((String, String, String, String), Int)] = adClickData_ds.map(data => {
      val sdf: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd")
      val day: String = sdf.format(new Date(data.ts.toLong))
      val area: String = data.area
      val city: String = data.city
      val ad: String = data.ad
      ((day, area, city, ad), 1)
    }).reduceByKey(_ + _)

    // 将统计的 数据 插入 和 更新 到 数据表
    area_count_ds.foreachRDD(rdd => {
      rdd.foreachPartition(itear => {
        // 获取 连接
        val con: Connection = JDBCUtil.getConnection
        val sql01 =
          """
            |insert into sparkStreaming_area_city_ad_count(dt,area,city,adid,count)
            |VALUES(?,?,?,?,?)
            |on DUPLICATE key UPDATE count = count + ?
            |""".stripMargin
        itear.foreach {
          case ((day, area, city, ad), count) => {
            JDBCUtil.executeUpdate(con, sql01, Array(day, area, city, ad, count, count))
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
