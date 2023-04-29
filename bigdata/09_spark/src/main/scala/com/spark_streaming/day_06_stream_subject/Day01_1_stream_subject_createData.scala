package com.spark_streaming.day_06_stream_subject

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}

import java.util.Properties
import scala.collection.mutable.ListBuffer
import scala.util.Random

/**
 * Created with IntelliJ IDEA.
 * Description:bigdata
 *
 * @Author: sky
 *          项目
 *          模拟 数据生成
 */
object Day01_1_stream_subject_createData {
  def main(args: Array[String]): Unit = {
    /**
     * 获取 生成数据的，向 kafka 推
     * 格式 ：timestamp area city userid adid
     * 含义： 时间戳    区域  城市  用户  广告
     */
    // 创建配置对象
    val prop = new Properties()
    // 添加配置
    prop.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "hdp180:9092")
    prop.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
    prop.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
    // 创建生产者 发送消息的k v 都是string类型，加载property 配置
    val producer: KafkaProducer[String, String] = new KafkaProducer[String, String](prop)

    while (true) {
      mockData().foreach(data => {
        // 创建 record 对象，参数是 topic 主题 和 数据 value
        val record: ProducerRecord[String, String] =
          new ProducerRecord[String, String]("spark_streaming001", data)
        producer.send(record) // 推送到 kafka
      })
      Thread.sleep(5000)
    }


  }
  /**
   * 模拟数据 主方法, 没 次 生产30条
   * 格式 ：timestamp area city userid adid
   * 含义： 时间戳    区域  城市  用户  广告
   */
  def mockData() = {
    val list_data: ListBuffer[String] = ListBuffer[String]()
    val area_list: ListBuffer[String] = ListBuffer[String]("华北", "华东", "华南")
    val city_list: ListBuffer[String] = ListBuffer[String]("北京", "上海", "深圳")
    for (i <- 1 to 30) {
      val area: String = area_list(new Random().nextInt(3)) // 随机 获取区域
      val city: String = city_list(new Random().nextInt(3)) // 随机获取 城市
      val user_id = new Random().nextInt(6) + 1 // 随机生成 1-6 的数
      val ad_id = new Random().nextInt(6) + 1
      // 添加到list
      list_data.append(s"${System.currentTimeMillis()} ${area} ${city} ${user_id} ${ad_id}")
    }
    list_data
  }

}
