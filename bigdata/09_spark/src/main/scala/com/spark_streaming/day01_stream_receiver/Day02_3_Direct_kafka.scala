package com.spark_streaming.day01_stream_receiver

import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.mutable

/**
 * Created with IntelliJ IDEA.
 * Description:bigdata
 *
 * @Author: sky
 *          数据源 是kafka ， spark streaming 采集kafka 数据
 */
object Day02_3_Direct_kafka {
  def main(args: Array[String]): Unit = {
    // 创建 环境 配置对象
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("kafka")
    // 创建 上下文 采集器 对象
    val ssc: StreamingContext = new StreamingContext(conf, Seconds(3))

    // 定义Kafka参数
    val kafkaPara: Map[String, Object] = Map[String, Object](
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG ->
        "hdp180:9092,hdp181:9092,hdp182:9092",
      ConsumerConfig.GROUP_ID_CONFIG -> "xc",
      "key.deserializer" ->
        "org.apache.kafka.common.serialization.StringDeserializer",
      "value.deserializer" ->
        "org.apache.kafka.common.serialization.StringDeserializer"
    )


    // 使用 kafka 工具类创，泛型 是指 kafka 数据的 kv类型
    val kafkaDataDS: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream[String, String](
      ssc, // 上下文 采集对象
      LocationStrategies.PreferConsistent, // 位置策略，采集数据 和 计算数据的节点分配策略，此处失败交给框架自动选择
      // 消费者 策略，数据泛型，参数是 主题topic 和 kafka参数,主题 先创建
      ConsumerStrategies.Subscribe[String, String](Set("spark_streaming001"), kafkaPara)
    )
    kafkaDataDS.map(_.value()).print()

    // 启动 采集器
    ssc.start()


    // 等待关闭 采集器
    ssc.awaitTermination()
  }

}
