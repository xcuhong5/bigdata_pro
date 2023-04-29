package com.xc.spark_tuning.cache

import com.xc.spark_tuning.utils.InitUtil
import org.apache.spark.SparkConf
import org.apache.spark.sql.{Row, SparkSession}

/**
 * rdd 原生的形式 进行java序列化缓存，结果是1比1的缓存，非常占用内存资源，普通测试机器几乎无法完成
 * 需要占用7g
 */
object RddCacheDemo {

  def main( args: Array[String] ): Unit = {
    val sparkConf = new SparkConf().setAppName("RddCacheDemo")
    val sparkSession: SparkSession = InitUtil.initSparkSession(sparkConf)

    val result = sparkSession.sql("select * from sparktuning.course_pay").rdd
    result.cache()
    result.foreachPartition(( p: Iterator[Row] ) => p.foreach(item => println(item.get(0))))
    while (true) {
      //因为历史服务器上看不到，storage内存占用，所以这里加个死循环 不让sparkcontext立马结束
    }
  }

}
