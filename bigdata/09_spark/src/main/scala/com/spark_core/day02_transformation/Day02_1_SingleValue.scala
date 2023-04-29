package com.spark_core.day02_transformation

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import java.text.SimpleDateFormat
import java.util.Date

/**
 * Created with IntelliJ IDEA.
 * Description:bigdata
 *
 * @Author: sky
 *          4. RDD算子 - 转换函数，旧 rdd 逻辑封装成 新的 rdd -- 双值 的转换函数（ 单数据源操作是单值，多数据源操作是双值，key-value操作）
 *          map  一个一个数据执行 逻辑
 *          mapPartitions 传入输出都是迭代器，获取分区所有数据加载到内存进行引用 执行逻辑，完成不会释放内存 （性能强于 map，但是 容易内存溢出）
 *          max 获取最大值
 *          mapPartitionsWithIndex 获取分区索引， 输入输出是（分区索引，迭代器）
 *          flatMap 平坦化，整体拆分 成 个体
 *          glom  将一个分区数据 转成 数组;分区不变
 *          groupBy 分区，传入的 是分组的 key
 *          filter 过滤 ，符合 条件的 保留
 *          sample 随机抽取
 *          distinct 去重
 *          coalesce 缩减+增加分区，默认是关闭 shuffle，增加分区需要开启 shuffle
 *          repartition 增加分区，自动shuffle，底层就是 coalesce；方便区分开
 *          sortBy 排序，默认 升序,第二个参数 分辨升序还是降序；有shuuffle 的过程
 */
object Day02_1_SingleValue extends App {
  /** map 转换函数 推倒式 */
  // 1. 创建 spark 配置对象，设置 master，设置 app 名
  val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("transF")
  // 2. 创建 spark driver 连接对象
  val sc: SparkContext = new SparkContext(sparkConf)
  /*
    map 是 一个数据 一个 数据的 执行逻辑，对内存不会造成影响
   */
  // 3. 文件 生成 rdd， 行为 单位; 设置 2个分区
  val log_rdd: RDD[String] = sc.textFile("09_spark/src/main/resources/test/apache.log", 2)
  // 4. 获取 内容中的 请求地址，根据空格拆分 集合中的第7个元素，下标是6
  val request_rdd: RDD[String] = log_rdd.map(line => line.split(" ")(6))
  // 5. 执行 动作函数 采集，触发 执行 程序
  request_rdd.collect().foreach(v => println(s"map 执行结果：${v}"))
  println()

  /** map 的性能 提升 => mapPartitions 分区为单位计算； 内存不足不推荐使用 */
  /*
    mapPartitions 性能 更强，是以分区为单位 执行 计算，而map 是一个一个数据执行
    缺点 是 mapPartitions 传入输出都是迭代器，将整个分区数据 加载内存 进行引用，计算完 不会释放，容易造成 内存溢出
   */
  val list_rdd: RDD[Int] = sc.makeRDD(List(1, 3, 5, 7, 9), 2)
  // mapPartitions 中 一个分区是一个迭代器，遍历迭代器数据 执行 *2 逻辑； 返回也是一个 迭代器
  val partition_rdd: RDD[Int] = list_rdd.mapPartitions(itera => {
    itera.map(_ * 2)
  })
  // 动作函数 触发执行程序
  partition_rdd.collect().foreach(v => println(s"mapPartitions 执行结果 ：${v}"))
  println()

  /** 迭代器 中的 max 函数，获取 分区中数据 最大值 */
  val max_rdd: RDD[Int] = partition_rdd.mapPartitions(itera => {
    List(itera.max).iterator // 获取 迭代器中的 最大值，通过封装list 返回迭代器类型
  })
  // 动作函数 触发 执行程序
  max_rdd.collect().foreach(m => println(s"获取分区 数据中 最大值：${m}"))
  println()

  /** mapPartitionsWithIndex 输入是（分区索引，迭代器） */
  val index_rdd: RDD[Int] = sc.makeRDD(List(1, 3, 5, 7, 9), 2)
  // 1. 获取 分区索引 和 数据
  val numData_rdd: RDD[(Int, Int)] = index_rdd.mapPartitionsWithIndex(
    (index, datas) => {
      datas.map((index, _)) // 封装 分区索引 和 数据
    }
  )
  numData_rdd.collect().foreach(data => {
    println(s"分区索引：${data._1} => 数据：${data._2}")
  })

  // 2. 只 获取 第二个分区的 数据
  val get2Index_rdd: RDD[Int] = index_rdd.mapPartitionsWithIndex((index, itera) => {
    if (index == 1) { // 判断 分区索引，值获取第2个分区数据表
      itera
    } else {
      Nil.iterator // 否则 返回 空的 迭代器
    }
  })
  // 动作函数 触发 执行
  get2Index_rdd.collect().foreach(two => {
    println(s"获取第二个分区的 数据：${two}")
  })
  println()

  /** *** flatMap 平坦化，整体拆分 成 个体；输入输出 都是 list ***** */
  val flatMap_rdd: RDD[List[Int]] = sc.makeRDD(
    List(List(1, 2), List(3, 4))
  )
  // 先map 在 flat，参数是List(List(1, 2), List(3, 4))，平坦后 返回list(1,2,3,4)
  val listFlat_rdd: RDD[Int] = flatMap_rdd.flatMap(list => list)
  // 动作函数 触发 执行
  listFlat_rdd.collect().foreach(coll => println(s"flatMap平坦化结果 ： ${coll}"))

  // 案列2 平坦化 ，单词
  val wd_rdd: RDD[String] = sc.makeRDD(List("hello scala", "nice to me too scala"))
  // 先map 入参 一个衣蛾的元素，根据 空格 分割，返回一个 可迭代的集合 即可
  val word_rdd: RDD[String] = wd_rdd.flatMap(word => {
    word.split(" ")
  })
  // 动作函数 ;  触发 执行
  word_rdd.collect().foreach(wd => println(s"flatMap 单词 平坦化： ${wd}"))

  // 案列3 ；杂乱的  数据  做平坦化
  val fm_rdd: RDD[Any] = sc.makeRDD(
    List(List(1, 2), 3, List(4, 5), 6, 7)
  )
  // 通过 模式匹配 实现
  val any_rdd: RDD[Any] = fm_rdd.flatMap(data => {
    data match {
      case list: List[_] => list // 匹配 到list 返回 list
      case value => List(value) // 匹配到 单值的 转成 list
    }
  })
  // 动作函数 触发 执行
  any_rdd.collect().foreach(data => println(s"杂乱数据的 平坦化：${data}"))
  println()

  /** **** glom  将一个分区数据 转成 数组 ;分区不变******* */
  // 实现 获取分区中的 最大值，分区之间最大值 求和
  val g_rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4), 2) // 两个 分区
  val glom_rdd: RDD[Array[Int]] = g_rdd.glom() // 将分区数据 转数组，两个分区就是两个数组
  val max_grdd: RDD[Int] = glom_rdd.map(arr => { // 分区已经转成了 数组，获取数组最大值
    arr.max
  })
  // 分区之间最大值就是 数组之间 最大值 ，求和
  println("分区之间 最大值求和：" + max_grdd.collect().sum)
  println()

  /** **** groupBy 分区，传入的 是分组的 key ************ */
  // 案列1 ： 单双数 分组
  val groupBy_rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4), 2)
  val num_rdd: RDD[(Int, Iterable[Int])] = groupBy_rdd.groupBy(_ % 2) // 分组，单数 双数 分组
  num_rdd.collect().foreach(n => println(s"groupBy 分区 单双数分组：${n}"))

  // 案列2 ： 单词 分组
  val gp_rdd: RDD[String] = sc.makeRDD(List("hello", "scala", "hello", "spark"), 2)
  val wdgp_rdd: RDD[(String, Iterable[String])] = gp_rdd.groupBy(wd => wd)
  wdgp_rdd.collect().foreach(wd => println(s"根据集合 元素 分组：${wd}"))

  // 案列3 ： 根据 单词 首字母 分组
  val first_rdd: RDD[(Char, Iterable[String])] = gp_rdd.groupBy(_.charAt(0))
  first_rdd.collect().foreach(f => println(s"根据元素 首字母 分组：${f}"))

  // 案列4 ： 获取 日志 中 时间段 的 访问量，不限日期只根据 时段（单词计数，有多种实现，此处只用 groupby 实现）
  val date_rdd: RDD[String] = sc.textFile("09_spark/src/main/resources/test/apache.log")
  val hour_rdd: RDD[(String, Int)] = date_rdd.map(line => {
    val time: String = line.split(" ")(3) // 根据空格 拆分，获取 时间
    val dft_dates: SimpleDateFormat = new SimpleDateFormat("dd/MM/yy:HH:mm:ss") //时间格式化
    val date: Date = dft_dates.parse(time) // 字符转时间
    val dft: SimpleDateFormat = new SimpleDateFormat("HH") // 时间格式化，格式化 出 时段
    val hour: String = dft.format(date) // 获取 时间的 时段
    (hour, 1) // 封装 成元祖，做分组统计
  })
  // 分组 统计 出 时段的 访问量，就是统计 时段
  val count_rdd: RDD[(String, Int)] = hour_rdd.groupBy(_._1).map {
    case (hour, itera) => (hour, itera.size)
  }
  count_rdd.collect().foreach(c => println(s"统计 时段 的 访问量：${c}"))
  println()

  /** filter 过滤 ，符合 条件的 保留  * */
  // 过滤出  17/05/2015 的 数据
  val filters: RDD[String] = sc.textFile("09_spark/src/main/resources/test/apache.log")
  val filter_rdd: RDD[String] = filters.filter(lines => {
    val time: String = lines.split(" ")(3) // 获取 时间
    time.startsWith("17/05/2015") // 只保留 这个 时间 开始的 数据
  })
  filter_rdd.collect().foreach(f => println(s"filter 过滤 出 2015-5-17 的数据：${f}"))
  println()

  /** sample 随机抽取 *** */
  val dataRDD = sc.makeRDD(List(1, 2, 3, 4), 1)
  // 抽取数据不放回（伯努利算法）
  // 伯努利算法：又叫0、1分布。例如扔硬币，要么正面，要么反面。
  // 具体实现：根据种子和随机算法算出一个数和第二个参数设置几率比较，小于第二个参数要，大于不要
  // 第一个参数：抽取的数据是否放回，false：不放回
  // 第二个参数：抽取的几率，范围在[0,1]之间,0：全不取；1：全取；
  // 第三个参数：随机数种子
  val dataRDD1 = dataRDD.sample(false, 0.5)
  println("随机抽取 不放回：" + dataRDD1.collect().mkString(","))
  // 抽取数据放回（泊松算法）
  // 第一个参数：抽取的数据是否放回，true：放回；false：不放回
  // 第二个参数：重复数据的几率，范围大于等于0.表示每一个元素被期望抽取到的次数
  // 第三个参数：随机数种子
  val dataRDD2 = dataRDD.sample(true, 2)
  println("随机抽取 放回：" + dataRDD2.collect().mkString(","))
  println()

  /** ** distinct 去重  *** */
  val dist_rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 1, 2, 6, 5))
  val distinct_rdd: RDD[Int] = dist_rdd.distinct()
  distinct_rdd.collect().foreach(dis => println(s"distinct 去重 ： ${dis}"))
  println()

  /** * coalesce 缩减+增加分区，默认是关闭 shuffle，增加分区需要开启 shuffle
   * 容易导致数据倾斜，大量数据时可以开启，分区数据重新分配,数据会被打乱
   * 用于 数据小 分区多的场景，避免调度时间 大于 执行时间
   * coalesce(缩减分区数，是否shuffle默认false)
   */
  val coal_rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4, 5, 6, 7, 8, 9), 4)
  val colaes_rdd: RDD[Int] = coal_rdd.coalesce(3) // 缩减到 3 个分区
  colaes_rdd.saveAsTextFile("09_spark/src/main/resources/test/out/coalesce_out")
  val colaes_rdd2: RDD[Int] = coal_rdd.coalesce(3, true) // 缩减到 2 个分区,执行shuffle
  colaes_rdd2.saveAsTextFile("09_spark/src/main/resources/test/out/coalesce_out2")
  // 扩大分区 需要 开启 shuffle，分区数据重新分配，数据会被打乱
  val addPar_rdd: RDD[Int] = colaes_rdd.coalesce(5, true)
  addPar_rdd.saveAsTextFile("09_spark/src/main/resources/test/out/coalesce_out3")
  println()

  /** ** repartition 增加分区，自动shuffle，底层就是 coalesce；方便区分开 * */
  val rePa_rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4, 5, 6, 7, 8, 9), 2)
  val addPartion_rdd: RDD[Int] = rePa_rdd.repartition(3) // 增加分区
  addPartion_rdd.saveAsTextFile("09_spark/src/main/resources/test/out/repartition_out")
  println()

  /** sortBy 排序，默认 升序,第二个参数 分辨升序还是降序；有shuuffle 的过程 *** */
  val sort_rdd: RDD[Int] = sc.makeRDD(List(7, 8, 6, 9, 1, 5, 6), 2)
  val sortAsc_rdd: RDD[Int] = sort_rdd.sortBy(num => num) // 默认 升序
  sortAsc_rdd.saveAsTextFile("09_spark/src/main/resources/test/out/sortAsc_out")
  val sortDesc_rdd: RDD[Int] = sort_rdd.sortBy(num => num, false) // 降序 排序
  sortDesc_rdd.saveAsTextFile("09_spark/src/main/resources/test/out/sortDesc_out")
  // 执行 元素 进行 排序
  val sort2_rdd: RDD[(String, Int)] = sc.makeRDD(List(("1", 1), ("11", 2), ("2", 3)))
  val s2_rdd: RDD[(String, Int)] = sort2_rdd.sortBy(_._1.toInt) // 字符串排序 是根据第一个字符排序，所以"11"不是最大，需要转int
  s2_rdd.collect().foreach(s => println(s"根据 指定元素排序：${s}"))

  // 6. 关闭 spark 对象
  sc.stop()


}
