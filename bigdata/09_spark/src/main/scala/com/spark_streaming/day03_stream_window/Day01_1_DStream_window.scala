package com.spark_streaming.day03_stream_window

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * Created with IntelliJ IDEA.
 * Description:bigdata
 *
 * @Author: sky
 *          window 窗口 函数
 */
object Day01_1_DStream_window {
  def main(args: Array[String]): Unit = {
    // 创建 配置对象
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("window")
    // 创建 采集器 对象
    val ssc: StreamingContext = new StreamingContext(conf, Seconds(2))
    ssc.checkpoint("file:/H:/cp")
    // 监听  端口数据
    val data9: ReceiverInputDStream[String] = ssc.socketTextStream("hdp180", 9999)
    val wc_ds: DStream[(String, Int)] = data9.map((_, 1))
    /**
     * window()
     * 第一个参数是 窗口时长 滑窗的间隔时间（是采集周期时间的整数倍）
     * 第二个参数是 滑动步长。计算周期。每6秒计算一次前4秒的 数据
     * 一般会设置 两个参数一样，如果此处的 每6秒计算前4秒数据，则会漏2秒数据。按照实际业务设置
     */
    val wordCont: DStream[(String, Int)] = wc_ds.window(Seconds(4), Seconds(6)).reduceByKey(_ + _)
    wordCont.print()

    // 采集器 开始 和 等待 关闭
    ssc.start()
    ssc.awaitTermination()
  }
}
