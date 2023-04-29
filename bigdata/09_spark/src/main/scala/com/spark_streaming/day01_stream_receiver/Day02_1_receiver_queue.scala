package com.spark_streaming.day01_stream_receiver

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.mutable


/**
 * Created with IntelliJ IDEA.
 * Description:bigdata
 *
 * @Author: sky
 *      将 Queue 作为数据源 采集数据,创建 DStream
 */
object Day02_1_receiver_queue {
  def main(args: Array[String]): Unit = {
    //1.初始化Spark配置信息
    val conf = new SparkConf().setMaster("local[*]").setAppName("RDDStream")
    //2.初始化SparkStreamingContext
    val ssc = new StreamingContext(conf, Seconds(2))

    //3.创建RDD队列
    val rddQueue = new mutable.Queue[RDD[Int]]()

    //4.创建QueueInputDStream
    val inputStream = ssc.queueStream(rddQueue, oneAtATime = false)
    //5.处理队列中的RDD数据
    val mappedStream = inputStream.map((_, 1))
    val reducedStream = mappedStream.reduceByKey(_ + _)
    //6.打印结果
    reducedStream.print()

    //7.启动任务
    ssc.start()

    //8.循环创建并向RDD队列中放入RDD
    for (i <- 1 to 5) {
      rddQueue += ssc.sparkContext.makeRDD(1 to 300, 10)
      Thread.sleep(2000)
    }
    ssc.awaitTermination()

  }
}
