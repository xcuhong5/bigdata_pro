package com.xc.spark_tuning.cache

import com.xc.spark_tuning.bean.CoursePay
import com.xc.spark_tuning.utils.InitUtil
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
 * DataFrame、DataSet 的 缓存，和rdd 不同，数据7g 占用了 320m
 */
object DatasetCacheDemo {

  def main( args: Array[String] ): Unit = {
    val sparkConf = new SparkConf().setAppName("DataSetCacheDemo")
//      .setMaster("local[*]")
    val sparkSession: SparkSession = InitUtil.initSparkSession(sparkConf)


    import sparkSession.implicits._
    val result = sparkSession.sql("select * from sparktuning.course_pay").as[CoursePay]
    result.cache() // 默认是 内存和磁盘，内存不够就缓存 到磁盘
    result.foreachPartition(( p: Iterator[CoursePay] ) => p.foreach(item => println(item.orderid)))
    while (true) {
    }

  }

}
