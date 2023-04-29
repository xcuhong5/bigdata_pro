package com.spark_streaming.day02_stream_state

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * Created with IntelliJ IDEA.
 * Description:bigdata
 *
 * @Author: sky
 *          join 连接，两个流数据进行连接。要求kv型，两个流的 批次大小一致
 */
object Day01_3_DStream_join {
  def main(args: Array[String]): Unit = {
    // 创建 配置对象
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("join")
    // 创建 采集器 对象
    val ssc: StreamingContext = new StreamingContext(conf, Seconds(6))

    // 监听 两个 端口数据 ，进行 join
    val data6: ReceiverInputDStream[String] = ssc.socketTextStream("hdp180", 6666)
    val data9: ReceiverInputDStream[String] = ssc.socketTextStream("hdp180", 9999)
    // 转成 kv 型 数据
    val ds6: DStream[(String, Int)] = data6.map((_, 6))
    val ds9: DStream[(String, Int)] = data9.map((_, 9))
    // join 操作就是相同的 key进行连接，（key，（v1，v2））
    val dsJoin: DStream[(String, (Int, Int))] = ds6.join(ds9)

    dsJoin.print()

    // 开启 采集器
    ssc.start()
    // 等待 关闭采集器
    ssc.awaitTermination()


  }
}
