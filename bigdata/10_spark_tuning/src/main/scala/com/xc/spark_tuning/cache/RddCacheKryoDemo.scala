package com.xc.spark_tuning.cache

import com.xc.spark_tuning.bean.CoursePay
import com.xc.spark_tuning.utils.InitUtil
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel

/**
 * rdd 缓存，是 kryo 序列化 缓存，内存资源占用 是没有使用工具的 7分之1
 * 结果占用1g
 */
object RddCacheKryoDemo {


  def main( args: Array[String] ): Unit = {
    val sparkConf = new SparkConf()
      .setAppName("RddCacheKryoDemo")
//      .setMaster("local[*]")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .registerKryoClasses(Array(classOf[CoursePay]))


    val sparkSession: SparkSession = InitUtil.initSparkSession(sparkConf)

    import sparkSession.implicits._
    val result = sparkSession.sql("select * from sparktuning.course_pay").as[CoursePay].rdd
    result.persist(StorageLevel.MEMORY_ONLY_SER)
    result.foreachPartition(( p: Iterator[CoursePay] ) => p.foreach(item => println(item.orderid)))

    while (true) {
      //因为历史服务器上看不到，storage内存占用，所以这里加个死循环 不让sparkcontext立马结束
    }
  }


}
