package com.spark_streaming.day01_stream_receiver

import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.dstream.ReceiverInputDStream
import org.apache.spark.streaming.receiver.Receiver
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.util.Random

/**
 * Created with IntelliJ IDEA.
 * Description:bigdata
 *
 * @Author: sky
 *          自定义 数据源，创建 DStream
 */
object Day02_2_receiver_DIY {
  def main(args: Array[String]): Unit = {
    // 创建 配置 对象
    val sprkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("diy")
    // 创建 上下文 采集器 对象
    val ssc: StreamingContext = new StreamingContext(sprkConf, Seconds(3))


    val mesg_DStream: ReceiverInputDStream[String] = ssc.receiverStream(new MyReceiver())
    mesg_DStream.print()

    // 启动 采集器
    ssc.start()

    // 等待关闭 采集器
    ssc.awaitTermination()
  }

  /**
   * 自定义 数据源
   * 1. 继承 Receiver 定义泛型，传递参数 数据存储类型
   * 2. 重写方法
   */
  class MyReceiver extends Receiver[String](StorageLevel.MEMORY_ONLY) {
    var flag = true // 控制产生数据的 循环 变量

    // 启动时,生产数据
    override def onStart(): Unit = {
      // 此处测试是写一个线程 一直写数据
      new Thread(new Runnable {
        override def run(): Unit = {
          while (flag) {
            val mesg: String = new Random().nextInt(10).toString
            store(mesg) // 底层方法 存储数据
            Thread.sleep(500)
          }
        }
      }).start()
    }

    // 关闭时
    override def onStop(): Unit = {
      flag = false // 结束时 停止产生数据
    }
  }
}
