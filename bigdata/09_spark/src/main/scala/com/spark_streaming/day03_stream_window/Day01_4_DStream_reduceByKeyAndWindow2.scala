package com.spark_streaming.day03_stream_window

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * Created with IntelliJ IDEA.
 * Description:bigdata
 *
 * @Author: sky
 *          reduceByKeyAndWindow  窗口 函数, 窗口内 key 相同的 value 进行聚合
 *          此处是增强版，有删除 ,当窗口范围比较大，但是滑动幅度比较小时 可以增加清除重复数据的功能
 *          需要 checkpoint
 */
object Day01_4_DStream_reduceByKeyAndWindow2 {
  def main(args: Array[String]): Unit = {
    // 创建 配置对象
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("reduceByWindow2 ")
    // 创建 采集器 对象
    val ssc: StreamingContext = new StreamingContext(conf, Seconds(2))
    ssc.checkpoint("file:/H:/tmp/cp")
    // 监听  端口数据
    val data9: ReceiverInputDStream[String] = ssc.socketTextStream("hdp180", 9999)
    val wd_num: DStream[(String, Int)] = data9.map((_, 1))
    /**
     * reduceByWindow()
     * 窗口 内元素的 做聚合， key 相同的 value 进行聚合
     */
    val wc_ds: DStream[(String, Int)] = wd_num.reduceByKeyAndWindow(
      (x: Int, y: Int) => x + y,
      (x: Int, old: Int) => x - old,
      Seconds(4), Seconds(2))
    wc_ds.print()


    // 采集器 开始 和 等待 关闭
    ssc.start()
    ssc.awaitTermination()
  }
}
