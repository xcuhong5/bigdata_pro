package com.spark_core.day02_transformation

import com.spark_core.day02_transformation.Day02_3_KeyValue.foldKey_rdd
import org.apache.spark.rdd.RDD
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}

/**
 * Created with IntelliJ IDEA.
 * Description:bigdata
 *
 * @Author: sky
 *          6. RDD算子 - 转换函数，旧 rdd 逻辑封装成 新的 rdd --key-value 的转换函数（ 单数据源操作是单值，多数据源操作是双值，key-value操作）
 *          partitionBy 重新分区
 *          reduceByKey key 相同 进行操作；shuff之前 在分区内会预先处理 相同key的数据，减少落盘数据量，groupByKey
 *          groupByKey 根据key 进行 分组，结果是对偶 元祖
 *          aggregateByKey( 默认值 )(分区内计算规则，分区间计算规则),对 相同key 进行操作
 *          foldByKey(默认值)(计算规则)； 分区内 和 分区间 计算规则一样 可以使用 此函数
 *          sortByKey 根据 key 排序 ，true 正序，false 倒序
 *          join 数据源之间 相同的 key 进行 连接，强关联，没有匹配的 不会计算,key重复会依次匹配
 *          leftOuterJoin 左连接，相同的key 连接，以主表数据为主
 *          cogroup 分组 连接；相同的 key在一个组进行连接
 *          案列 ： 统计 每个省份 每个广告 被点击排行 top3
 */
object Day02_3_KeyValue extends App {
  // 创建 spark 配置对象 ，设置master和 app name
  val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("kv")
  // 创建 spark driver 对象
  val sc: SparkContext = new SparkContext(sparkConf)

  // 创建 kv 型 数据源
  val kv_rdd: RDD[(String, Int)] = sc.makeRDD(List(("a", 2), ("b", 3), ("a", 5)), 2)

  // partitionBy 重新分区，如果重新分区和前分区类型一致，不会生效
  //  kv_rdd.saveAsTextFile("09_spark/src/main/resources/test/out/k_v_partitionBy") //避免运行 错误 注释
  //  kv_rdd.partitionBy(new HashPartitioner(2)).saveAsTextFile("09_spark/src/main/resources/test/out/k_v_partitionBy_mew")

  // reduceByKey key 相同 进行操作；shuff之前 在分区内会预先处理 相同key的数据，减少落盘数据量，groupByKey
  val reduceByKey_rdd: RDD[(String, Int)] = kv_rdd.reduceByKey(_ + _)
  reduceByKey_rdd.collect().foreach(v => println(s" reduceByKey 根据key 相同元素进行操作：${v} "))
  println()

  // groupByKey 根据key 进行 分组，结果是对偶 元祖，(key,对应value 集合)；和 group 的区别是 group 是(key,(k,v))
  val groupByKey_rdd: RDD[(String, Iterable[Int])] = kv_rdd.groupByKey()
  groupByKey_rdd.collect().foreach(v => println(s"groupByKey 相同的 key，放在一个组中，对偶集合：${v}"))
  println()

  /*
    aggregateByKey()() ;两个参数
      第一个参数参数列表：默认值,默认值根据计算规则 实现累加 求最大...
      第二个参数列表中有两个参数：( 分区内计算规则，分区间 计算规则 )
   */
  val kv_rdd3: RDD[(String, Int)] = sc.makeRDD(List(("a", 2), ("b", 3), ("a", 5), ("b", 5)), 2)
  // 案列1： 默认值0 ，实现 分区内和 分区间 相同的 key 聚合；此处分区内和分区间计算规则一样
  val aggregateByKey_rdd: RDD[(String, Int)] = kv_rdd3.aggregateByKey(0)(_ + _, _ + _)
  aggregateByKey_rdd.collect().foreach(v => println(s"aggregateByKey 聚合：${v}"))
  // 案列2： 默认值10，实现 相同 key 分区内求最大值，分区间聚合
  val aggregateByKey_rdd02: RDD[(String, Int)] = kv_rdd3.aggregateByKey(3)(
    (x, y) => math.max(x, y),
    (x, y) => x + y
  )
  aggregateByKey_rdd02.collect().foreach(v => println(s"分区内最大值，分区间聚合：${v}"))

  // 案列 3 ： 实现 相同key 的平均值
  val avg_rdd: RDD[(String, (Int, Int))] = kv_rdd3.aggregateByKey((0, 0))(
    /*
    (0, 0) 默认值,用于记录 相同key 的聚合 和 key 数量
    第一个 表示 分区数据中相同key 的v  ；  第二个表示 统计相同key的个数
    (tuples, value) 表示 默认值 和 相同key的 v 数据
    (tuples._1 + value, tuples._2 + 1)
    表示 默认值+v , 统计相同 key个数
           */
    (tuples, value) => {
      (tuples._1 + value, tuples._2 + 1)
    },
    /*
     分区间 的数据，分区间的 v 相聚合，分区间的 相同 key聚合
     返回结果 相同key 聚合结果 和 key数量 ： (b,(8,2))
                (a,(7,2))
     */
    (tuples01, tuples02) => (tuples01._1 + tuples02._1, tuples01._2 + tuples02._2)
  )
  // 相同key 的聚合 结果 / key 数量; 应用于 key =>(value 是集合)
  val avg_rdd02: RDD[(String, Int)] = avg_rdd.mapValues(t => t._1 / t._2)
  avg_rdd02.collect().foreach(v => println(s"相同key 的平均值：${v}"))
  println()

  // foldByKey(默认值)(计算规则)； 分区内 和 分区间 计算规则一样 可以使用 此函数
  val foldKey_rdd: RDD[(String, Int)] = sc.makeRDD(List(("a", 2), ("b", 4), ("a", 5), ("b", 5)), 2)
  val foldKey_rdd02: RDD[(String, Int)] = foldKey_rdd.foldByKey(0)(_ + _)
  foldKey_rdd02.collect().foreach(v => println(s"foldByKey 区内 和 分区间 计算规则一样:${v}"))
  println()

  // sortByKey 根据 key 排序 ，true 正序，false 倒序
  val sortByKey_rdd: RDD[(String, Int)] = sc.makeRDD(List(("x", 5), ("g", 7), ("a", 1), ("e", 3)), 2)
  val asc_rdd: RDD[(String, Int)] = sortByKey_rdd.sortByKey(true)
  asc_rdd.collect().foreach(v => println(s"sortByKey 根据key排序 正序排：${v}"))
  val desc_rdd2: RDD[(String, Int)] = sortByKey_rdd.sortByKey(false)
  desc_rdd2.collect().foreach(v => println(s"sortByKey 根据key排序 倒序排：${v}"))
  println()

  /*
   join 数据源之间 相同的 key 进行 连接，内关联，没有匹配的 不会计算,key重复会依次匹配
   返回结果 (1,(a,4))
   */
  val join_rdd1: RDD[(Int, String)] = sc.makeRDD(Array((1, "a"), (2, "b"), (3, "c"), (3, "c")))
  val join_rdd2: RDD[(Int, Int)] = sc.makeRDD(Array((1, 4), (2, 5), (3, 6)))
  val join_rdd: RDD[(Int, (String, Int))] = join_rdd1.join(join_rdd2)
  join_rdd.collect().foreach(v => println(s"join 内连接连接，相同key连接：${v}"))
  println()

  /* leftOuterJoin 左连接，相同的key 连接，以主表数据为主
  返回结果 (1,(a,Some(4)))
  */
  val leftJoin_rdd1: RDD[(Int, String)] = sc.makeRDD(Array((1, "a"), (2, "b"), (4, "c")))
  val leftJoin_rdd2: RDD[(Int, Int)] = sc.makeRDD(Array((1, 4), (2, 5), (3, 6)))
  val leftjoin_rdd: RDD[(Int, (String, Option[Int]))] = leftJoin_rdd1.leftOuterJoin(leftJoin_rdd2)
  leftjoin_rdd.collect().foreach(v => println(s"leftOuterJoin 左外连接，相同key连接：${v}"))
  println()

  /*
   cogroup 分组 连接；相同的 key在一个组进行连接
   返回结果：   cogroup : (a,(CompactBuffer(1),CompactBuffer(3)))
                cogroup : (b,(CompactBuffer(2),CompactBuffer(4, 4)))
                cogroup : (c,(CompactBuffer(3),CompactBuffer()))
                cogroup : (d,(CompactBuffer(),CompactBuffer(2)))
   */
  val coGroup_rdd1: RDD[(String, Int)] = sc.makeRDD(List(("a", 1), ("b", 2), ("c", 3)))
  val coGroup_rdd2: RDD[(String, Int)] = sc.makeRDD(List(("a", 3), ("b", 4), ("b", 4), ("d", 2)))
  val co_rdd: RDD[(String, (Iterable[Int], Iterable[Int]))] = coGroup_rdd1.cogroup(coGroup_rdd2)
  co_rdd.collect().foreach(v => println(s"cogroup : ${v}"))

  /** 案列 ： 统计 每个省份 每个广告 被点击排行 top3 */
  // 数据 agent.log => 1516609143867 6 7 64 16 ：》 时间戳 省  市  用户 广告
  val agent_rdd: RDD[String] = sc.textFile("09_spark/src/main/resources/test/agent.log")
  // 返回 结果： ((省份，广告),1),每个省 每个广告记录1 方便 统计
  val sf_gg_rdd: RDD[((String, String), Int)] = agent_rdd.map(line => {
    val arr_args = line.split(" ")
    ((arr_args(1), arr_args(4)), 1)
  })
  // key （省份，广告）相同 的 value 累加，结果 ((省份，广告),sum)
  val reduceLog_rdd: RDD[((String, String), Int)] = sf_gg_rdd.reduceByKey(_ + _)
  // 改变 数据的 位置 (省份,（广告，sum）)
  val mapLog_rdd: RDD[(String, (String, Int))] = reduceLog_rdd.map(line => {
    (line._1._1, (line._1._2, line._2))
  })
  // 对省份 分组，对广告点击数 排序 获取 前三
  val top3_rdd: RDD[(String, List[(String, Int)])] = mapLog_rdd.groupByKey().mapValues(iter => {
    iter.toList.sortBy(_._2).reverse.take(3)
  })

  top3_rdd.collect().foreach(v => println(s"获取 每个省 点击前三的 广告:${v}"))
  sc.stop()
}
