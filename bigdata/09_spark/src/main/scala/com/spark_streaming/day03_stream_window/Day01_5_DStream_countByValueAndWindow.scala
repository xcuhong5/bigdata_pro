package com.spark_streaming.day03_stream_window

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * Created with IntelliJ IDEA.
 * Description:bigdata
 *
 * @Author: sky
 *          countByValueAndWindow  窗口 函数, 统计元素相同的个数，返回 map(元素，个数)
 *          需要 checkpoint
 */
object Day01_5_DStream_countByValueAndWindow {
  def main(args: Array[String]): Unit = {
    // 创建 配置对象
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("countByValueAndWindow")
    // 创建 采集器 对象
    val ssc: StreamingContext = new StreamingContext(conf, Seconds(2))
    ssc.checkpoint("file:/H:/tmp/cp")
    // 监听  端口数据
    val data9: ReceiverInputDStream[String] = ssc.socketTextStream("hdp180", 9999)

    /**
     * countByValueAndWindow()
     * 统计元素相同的 个数 ，麻返回 map
     */
    val wc_ds: DStream[(String, Long)] = data9.countByValueAndWindow(Seconds(4), Seconds(6))
    wc_ds.print()


    // 采集器 开始 和 等待 关闭
    ssc.start()
    ssc.awaitTermination()
  }
}
