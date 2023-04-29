package com.spark_streaming.day01_stream_receiver

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * Created with IntelliJ IDEA.
 * Description:bigdata
 *
 * @Author: sky
 *          spark-streaming 的一个 单词统计wc；通过 端口一直发送数据做为数据源
 */
object Day01_1_wordCount {
  def main(args: Array[String]): Unit = {
    // 创建爱你 环境配置对象
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("streaming")
    // 创建 数据采集器对象, 传入配置对象，和 采集数据的周期，3秒采集一次数据
    val ssc: StreamingContext = new StreamingContext(sparkConf, Seconds(3))
    // 获取 端口数据
    val lines_dstream: ReceiverInputDStream[String] = ssc.socketTextStream("hdp180", 9999)
    // 单词统计
    val words: DStream[String] = lines_dstream.flatMap(_.split(" "))
    val word_num: DStream[(String, Int)] = words.map((_, 1))

    val word_count: DStream[(String, Int)] = word_num.reduceByKey(_ + _)
    word_count.print()
    // 启动采集器
    ssc.start()
    // 等待关闭 采集器对象，表示程序一致运行
    ssc.awaitTermination()
  }
}
