package com.spark_streaming.day02_stream_state

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * Created with IntelliJ IDEA.
 * Description:bigdata
 *
 * @Author: sky
 *          Transform 有状态 转换函数，在 某些功能 DStream无法实现则用  Transform
 *          如 updateStateByKey() transform() window部分函数，需要checkpoint一般都是有状态的
 */
object Day01_2_DStream_Transform {
  def main(args: Array[String]): Unit = {
    // 创建 配置 对象
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Transform")
    // 创建 采集器 对象
    val ssc: StreamingContext = new StreamingContext(conf, Seconds(3))
    // 开始 采集器
    ssc.start()
    val lines: ReceiverInputDStream[String] = ssc.socketTextStream("hdp180", 9999)
    val value: DStream[(String, Int)] = lines.transform(rdd => {
      val words: RDD[String] = rdd.flatMap(_.split(" "))
      val word_cont: RDD[(String, Int)] = words.map((_, 1))
      val wc: RDD[(String, Int)] = word_cont.reduceByKey(_ + _)
      wc
    })
    value.print()
    // 等待 关闭 采集器
    ssc.awaitTermination()
  }
}
