package com.spark_streaming.day02_stream_state

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * Created with IntelliJ IDEA.
 * Description:bigdata
 *
 * @Author: sky
 *          DStream 和 rdd 类似，可以转换成新的 DStream,
 *          转换函数 分为 无状态 和 有状态
 *          无状态 操作，只对 采集周期内的数据进行处理，采集周期之间的 操作结果相互无关联
 *          有状态 操作：采集周期 的结果会保留，然后 采集周期之间的结果 进行汇总;需要设置 checkpoint
 *          如 updateStateByKey() transform() window部分函数，需要checkpoint一般都是有状态的
 */
object Day01_1_DStream_State {
  def main(args: Array[String]): Unit = {
    // 创建 配置对象
    val sparkconf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("state")
    // 创建 上下文 采集对象,传入 配置和 采集数据时间
    val ssc: StreamingContext = new StreamingContext(sparkconf, Seconds(3))
    // 在 有状态 场景下，需要设置检查点，保存缓冲区的 状态数据
    ssc.checkpoint("file:/H:/cp")

    // 演示 监听端口 数据 ，做单词统计
    val line: ReceiverInputDStream[String] = ssc.socketTextStream("hdp180", 9999)
    /**
     * 无状态 操作，只对 采集周期内的数据进行处理，采集周期之间的 操作结果相互无关联
     * 有状态 操作：采集周期 的结果会保留，然后 采集周期之间的结果 进行汇总，需要设置 checkpoint
     */
    val ds_count: DStream[(String, Int)] = line.map((_, 1))
    // 无状态 场景
    //    val ds_wc: DStream[(String, Int)] = ds_count.reduceByKey(_ + _)
    //    ds_wc.print()

    /**
     * 有状态 场景，配置 checkpoint 使用
     * updateStateByKey ： 更新 相同 key 的 value
     * seq 是 新 采集周期 的相同key 的value 队列
     * buffer ： 是缓冲区 中的 数据
     * 原理则是 新的 v 序列总和 + 缓冲区的 v = 有状态汇总
     */
    val haveState_DS: DStream[(String, Int)] = ds_count.updateStateByKey(
      (seq: Seq[Int], buffer: Option[Int]) => {
        val newCount: Int = buffer.getOrElse(0) + seq.sum
        Option(newCount)
      })
    haveState_DS.print()
    // 开始 采集
    ssc.start()
    // 等待关闭 采集
    ssc.awaitTermination()
  }
}
