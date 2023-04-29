package com.spark_streaming.day03_stream_window

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * Created with IntelliJ IDEA.
 * Description:bigdata
 *
 * @Author: sky
 *          reduceByWindow  窗口 函数, 统计窗口内 元素的 个数
 */
object Day01_3_DStream_reduceByWindow {
  def main(args: Array[String]): Unit = {
    // 创建 配置对象
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("reduceByWindow ")
    // 创建 采集器 对象
    val ssc: StreamingContext = new StreamingContext(conf, Seconds(4))
    // 监听  端口数据
    val data9: ReceiverInputDStream[String] = ssc.socketTextStream("hdp180", 9999)

    /**
     * reduceByWindow()
     * 窗口 内元素的 做聚合，此处是 元素累加
     */
    val reduce_window_ds: DStream[String] = data9.reduceByWindow(_ + _, Seconds(4), Seconds(4))
    reduce_window_ds.print()

    // 采集器 开始 和 等待 关闭
    ssc.start()
    ssc.awaitTermination()
  }
}
