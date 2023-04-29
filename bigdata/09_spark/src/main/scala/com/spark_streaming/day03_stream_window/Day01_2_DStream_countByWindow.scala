package com.spark_streaming.day03_stream_window

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * Created with IntelliJ IDEA.
 * Description:bigdata
 *
 * @Author: sky
 *          countByWindow 窗口 函数, 统计窗口内 元素的 个数
 */
object Day01_2_DStream_countByWindow {
  def main(args: Array[String]): Unit = {
    // 创建 配置对象
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("countByWindow")
    // 创建 采集器 对象
    val ssc: StreamingContext = new StreamingContext(conf, Seconds(2))
    // 监听  端口数据
    val data9: ReceiverInputDStream[String] = ssc.socketTextStream("hdp180", 9999)
    val wc_ds: DStream[(String, Int)] = data9.map((_, 1))

    /**
     * countByWindow()
     * 统计 窗口 内 元素 个数
     */
    val cont_wiondow: DStream[Long] = wc_ds.countByWindow(Seconds(4), Seconds(6))
    cont_wiondow.print()

    // 采集器 开始 和 等待 关闭
    ssc.start()
    ssc.awaitTermination()
  }
}
