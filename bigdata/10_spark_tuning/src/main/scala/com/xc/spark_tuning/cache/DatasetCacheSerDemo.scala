package com.xc.spark_tuning.cache

import com.xc.spark_tuning.bean.CoursePay
import com.xc.spark_tuning.utils.InitUtil
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel

/**
 * DataSet 类似 RDD，但是并不使用 JAVA 序列化也不使用 Kryo 序列化，而是使用一种特有的编码器进行序列化对象
 */
object DatasetCacheSerDemo {

  def main( args: Array[String] ): Unit = {
    val sparkConf = new SparkConf()
      .setAppName("DatasetCacheSerDemo")
//      .setMaster("local[*]")
    val sparkSession: SparkSession = InitUtil.initSparkSession(sparkConf)

    import sparkSession.implicits._
    val result = sparkSession.sql("select * from sparktuning.course_pay").as[CoursePay]
    result.persist(StorageLevel.MEMORY_AND_DISK_SER)  // spark 自己实现的 序列化
    result.foreachPartition(( p: Iterator[CoursePay] ) => p.foreach(item => println(item.orderid)))

    while (true) {
      //因为历史服务器上看不到，storage内存占用，所以这里加个死循环 不让sparkcontext立马结束
    }
  }



}
