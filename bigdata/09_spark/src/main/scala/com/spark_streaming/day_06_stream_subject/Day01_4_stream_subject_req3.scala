package com.spark_streaming.day_06_stream_subject

import com.spark_streaming.day_06_stream_subject.util.JDBCUtil
import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}

import java.io.{File, FileWriter, PrintWriter}
import java.sql.Connection
import java.text.SimpleDateFormat
import java.util.Date
import scala.collection.mutable.ListBuffer


/**
 * Created with IntelliJ IDEA.
 * Description:bigdata
 *
 * @Author: sky
 *          需求3：
 *          从kafka 消费数据 主题是 spark_streaming001
 *
 *
 */
object Day01_4_stream_subject_req3 {
  def main(args: Array[String]): Unit = {
    // 创建 环境 配置对象
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("req3")
    // 创建 上下文 采集器 对象
    val ssc: StreamingContext = new StreamingContext(conf, Seconds(5))

    // 定义Kafka参数
    val kafkaPara: Map[String, Object] = Map[String, Object](
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> "hdp180:9092,hdp181:9092,hdp182:9092",
      ConsumerConfig.GROUP_ID_CONFIG -> "xc_req3",
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

    val res_ds: DStream[(Long, Int)] = adClickData_ds.map(data => {
      val ts: Long = data.ts.toLong
      val newTs: Long = ts / 10000 * 10000
      (newTs, 1)
    }).reduceByKeyAndWindow((x: Int, y: Int) => (x + y), Seconds(60), Seconds(10))
    res_ds.foreachRDD(rdd => {
      // 对rdd 数据 进行 时间 排序. 数据 拉取到driver
      val data: Array[(Long, Int)] = rdd.sortByKey(true).collect()
      // 装数据  的 list
      val list: ListBuffer[String] = ListBuffer[String]()
      data.foreach { // 遍历 排序后的 数据
        case (ts, count) => {
          // 时间格式化，只保留 分钟：秒
          val sdf: SimpleDateFormat = new SimpleDateFormat("mm:ss")
          val time: String = sdf.format(new Date(ts))
          // 将 每一条数据 添加到list
          list.append(s"""{"xtime":"${time}", "yval":"${count}"}""")
        }
      }
      // 将 list 数据 输出到文件
      val out: PrintWriter = new PrintWriter(
        new FileWriter(
          new File("F:/ide/bag/bigdata/09_spark/src/main/resources/adclick/adclick.json")))
      out.print("[" + list.mkString(",") + "]") // 将list 输出到文件，编写成 json格式
      out.flush() // 刷写
      out.close() // 关闭
    })

    // 启动 采集器
    ssc.start()
    // 等待关闭 采集器
    ssc.awaitTermination()
  }

  case class AdClickData(ts: String, area: String, city: String, userId: String, ad: String) // 数据封装对象
}
