package com.xcs.day02_collection

import scala.collection.{GenTraversableOnce, mutable}

/**
 * Created with IntelliJ IDEA.
 * Description:bigdata
 *
 * @Author: sky
 *          不可变 集合 List; 有序可重复
 */
object List_Immutable_03 extends App {
  // 创建 不可变集合
  val list = List(3, 5, 7)
  // 根据下标 获取 元素
  println("获取list 元素：" + list(1))
  // 遍历 集合
  for (elem <- list) {
    println(s"遍历 list 集合 ： ${elem}")
  }

  val size: Int = list.size // 获取 集合 长度
  val string: String = list.mkString // 集合 转 字符串
  val str1: String = list.mkString(",") // 集合 转 字符串 ,设置 分隔符
  println()

  val list14: List[Int] = list.updated(1, 8) // 根据下标 修改元素


  /** ** 添加元素 ***** */
  // 在 list 尾部 添加元素
  val list2: List[Int] = list :+ 15
  println(s"尾部新增 元素：${list2}")
  // 在 list 头部 添加元素
  val list3: List[Int] = 15 +: list
  println(s"头部新增 元素：${list3}")
  val any = "kaka" :: ("tom" :: Nil) :: (1 :: Nil)
  println(s"嵌套 集合:${any}")

  // Nil空集合， 1加入到nil头部，然后 26 加入头部 一次调用
  val ints: List[Int] = 15 :: 26 :: 1 :: Nil
  println(s"空集合 ： ${ints}")

  // 集合 合并
  val list4: List[Int] = ints ::: list3
  println(s"集合 合并 ： ${list4}")
  println()


  /** 查询 元素 * */
  val head: Int = list4.head //获取 头部第一个元素
  println(s"获取第一个元素：${head}")
  val tail: List[Int] = list4.tail // 获取除了第一个元素，剩下的 元素
  println(s"除去第一个元素 剩下的元素：${tail}")
  val last: Int = list4.last // 获取 最后一个元素
  println(s"获取最后一个元素：${tail}")
  val init: List[Int] = list4.init // 获取 除去最后一个 剩下的 元素
  println(s"除去最后一个元素 剩下的元素：${tail}")
  val reverse: List[Int] = list4.reverse // 倒序
  println(s"倒序：${list.reverse}")

  val empty: Boolean = list4.isEmpty // 判断集合 是否为空
  println()

  /** **获取 集合 最大值 最小元素 **** */
  val nums = List(5, 0, 1)
  println(s"获取 集合 最小元素：${nums.min}")
  println(s"获取 集合 最大元素：${nums.max}")
  println()

  /** ******** 判断 是否存在 ********** */
  val bool: Boolean = nums.contains()


  /** *map 的方式 遍历 集合, 返回新的 集合,也可以传入 一个函数 到map *** */
  val func = (a: Int) => a + 1
  val list1: List[Int] = list4.map(_ + 1) // 遍历元素，元素+1
  val list5: List[Int] = list4.map(func) // 传入 一个函数 到 map
  val str = List("sky 20 hk", "lisa 19 chengdu")
  // 只获取 人名
  val people: List[String] = str.map(_.split(" ")(0))
  // 遍历 集合 ，获取人名 和 年龄
  val tuples: List[(String, String)] = str.map(str => {
    val arr = str.split(" ")
    val user = arr(0)
    val age = arr(1)
    (user, age)
  })
  val list6: List[Array[String]] = str.map(_.split(" "))
  println("flatten 平坦化：" + list6.flatten) // flatten 将嵌套的集合，平坦化，变成一个整体集合
  val list7: List[String] = str.flatMap(_.split(" ")) // map + flatten 结合
  println()

  /** *******reduce 规约， 场景,叠加 ******* */
  val num = List(3, 7, 1, 10)
  val num2: Int = num.reduce(_ + _) // 累加操作，底层是reduceLeft ,左 往 右 加
  println(s"reduce 累加操作：${num2}")
  val i2: Int = num.reduceRight(_ - _) // 3-(7-(1-10))
  println(i2)

  val num3 = num.sum // 针对 数字 元素 有专门的 求和
  println(s"数字求和：${num3}")


  val i: Int = num.fold(5)(_ + _) // fold(5) 给一个初始值5，后面逻辑第一个参数是默认值，后面是集合的元素
  println(s"fold 初始值5：${i}")
  println()

  /** ********** mapValues 遍历 key 对应 的 value ************* */
  val mapv: mutable.Map[String, List[Int]] = mutable.Map("a" -> List(1, 2, 3), "b" -> List(3, 5))
  val map: collection.Map[String, Int] = mapv.mapValues(_.sum)
  println("mapValues 对集合 values 操作：" + map)
  println()

  /** ***********sort 排序************* */
  val users = List(("sky", 25), ("tom", 18))
  val sorteds: List[Int] = num.sorted // 默认 升序
  println(s"sorted 升序：${sorteds}")
  val list8: List[(String, Int)] = users.sortBy(v => v._2) // 指定 某个元素 升序 排序
  println(s"根据第二个元素升序排序：${list8}")
  val reverse1: List[(String, Int)] = users.sortBy(v => v._2).reverse // 倒序
  println(s"倒序 排序：${reverse1}")
  val list9: List[(String, Int)] = users.sortWith(_._2 > _._2) // 自定义排序, 根据第二个元素 降序
  println(s"自定义排序：${list9}")
  println()

  /** *****filter 过滤******* */
  val list10: List[Int] = num.filter(_ % 2 == 0) // 过滤 出 满足条件的 元素
  println(s"过滤 出 双数 ：${list10}")
  val list11: List[Int] = num.filterNot(_ % 2 == 0)
  println(s"不保留 双数 ：${list11}") // 过滤掉 满足条件的 元素
  val i1: Int = num.count(_ % 2 == 0) // 统计 满足条件的 元素 个数
  val length: Int = num.filter(_ % 2 == 0).length // 和 count 效果一样
  println(s"统计 满足条件的 元素个数：${i1}")
  println()

  /** ********集合 去重 底层 是 tostring，字符串 判断 *********** */
  val list12 = List(1, 1, 3).distinct // 只会 对上层元素 去重，嵌套的 集合 无效
  val list13 = List(List(1), List(1)).distinct
  println("去重 后的 集合：" + list12 + " => " + list13)
  println()


  /** ********** 集合与集合的  差集  交集  并集 ***** */
  val diff = List(1, 5, 9, 6)
  val diff2 = List(5, 8, 3, 1)
  val ints0: List[Int] = diff.diff(diff2) // 差集， diff 元素 不存在 diff2 中的元素
  println(s"集合之间的 差集：${ints0}")
  val ints1: List[Int] = diff.intersect(diff2) // 交集/并集, diff 元素  和 diff2 元素 相同的
  println(s"集合 之间的 并集：${ints1}")
  val ints2: List[Int] = diff ++ diff2
  diff.union(diff2) // 合并 集合，效果一样， ::: ++ union
  println(s"集合 合并：${ints2}")
  println()

  /** *********分组 group ************** */
  val stu = List(("sky", "boy", "hongkong"), ("tom", "boy", "hongkong"), ("lisa", "girl", "sc"), ("lisa", "boy", "sc"))
  // 根据某个元素 分组，结果 是map，分组条件的元素为key， 分组结果是list
  val stringToTuples: Map[String, List[(String, String, String)]] = stu.groupBy(_._2)
  println(s" 根据 元素的 值 进行分组：${stringToTuples}")
  // 将集合元素 模拟成字段，装进元祖，根据三元元组 最后一个元素分组
  val stringToList: Map[String, List[(String, String, String)]] = stu.groupBy { case (a, b, c) => c }
  println(s"匹配条件 进行分组：${stringToList}")
  // 两个元素 为一组，进行分组
  val toList1: List[List[(String, String, String)]] = stu.grouped(2).toList
  println(s"指定数量 分组：${toList1}")
  println()

  /** ***********span 将集合分成 两组******************* */
  val tmp = List(1, 1, 1, 2, 3, 6, 1)
  // 条件返回false 开始分组 分为两组，不会管后面=的元素是否满足
  val tuple: (List[Int], List[Int]) = tmp.span(_ == 1)
  println(s"返回false 开始分组，后面的 元素不管是否满足 ： ${tuple}")
  val tuple1: (List[Int], List[Int]) = tmp.splitAt(4) // 从第4 个元素开始 切分 分组， 一分为二
  println(s"根据 元素 位置分组， 一分为二：${tuple1}")
  val tuple2: (List[Int], List[Int]) = tmp.partition(_ == 1) // 会遍历整个集合，满足的一组，不满足的一组
  println(s"扫描所有元素，满足条件为一组：${tuple2}")
  println()


  /** **************sliding  将列表按照参数分组  步进 分组 ********************* */
  val test = List(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
  //sliding(2)  每组最多2个元素，还有一个参数默认为1  sliding(2,1)
  //List(List(1, 2), List(2, 3), List(3, 4), List(4, 5), List(5, 6), List(6, 7), List(7, 8), List(8, 9), List(9, 10))
  var sldOne = test.sliding(2).toList
  println("步进分组：" + sldOne)
  //每组最多元素2个  步进3个元素分一组 只会取里面左边两个元素
  //List(List(1, 2), List(4, 5), List(7, 8), List(10))
  var sldTwo = test.sliding(2, 3).toList
  println("步进：" + sldTwo)
  println()

  /** ********* take 提取元素********************** */
  val takes = List(1, 5, 1, 9, 6)
  val ints3: List[Int] = takes.take(2) // 提取 前面 2个元素
  println(s"提取 前面2 个 元素：${ints3}")
  val ints4: List[Int] = takes.takeRight(2) // 提取最后 两个元素
  println(s"提取 最后 2 个 元素：${ints4}")
  // 提取元素 直到条件不满足 停止（一个一个元素提取，遇到不匹配则结束）
  val ints5: List[Int] = takes.takeWhile(_ == 1)
  println("提取元素，直到条件返回false终止" + ints5)
  println()

  /** ************slice 根据下标 提取元素**************************** */
  val ints9: List[Int] = takes.slice(1, 3)
  println(s"提取元素，下标 1 到 3，不包含3：${ints9}")
  println()

  /** *********drop 丢弃元素****************** */
  val drops = List(1, 5, 1, 9, 6)
  val ints6: List[Int] = drops.drop(2) // 丢弃 头部 两个元素
  println(s"丢弃前面 2 个元素“${ints6}")
  val ints7: List[Int] = drops.dropRight(2) // 丢弃 最后 两个元素
  println(s"丢弃 最后 2 个元素“${ints7}")
  val ints8: List[Int] = drops.dropWhile(_ == 1) //  丢弃元素 直到条件不满足 停止（一个一个元素丢弃，遇到不匹配则结束）
  println(s"条件 返回 false 停止丢弃：${ints8}")
  println()

  /** **********padTo 集合长度 补齐************************ */
  val pad = List("a", "b")
  val strings: List[String] = pad.padTo(4, "n") // 集合长度不足4 ，n字符补齐
  println(s"元素长度补齐，指定 补齐元素：${strings}")
  println()

  /** ************* zip 拉链操作  集合与集合配对   ***************** */
  val zip_list = List("a", "b", "c")
  val pair = List(1, 2, 6, 7, 0)
  // 就像拉拉链一样,相互配对，两个集合 根据下标 相互配对，集合最短长度 为生成新集合的长度
  val zip01: List[(String, Int)] = zip_list.zip(pair)
  println("拉链配对,返回的数组为两个中最短的一个" + zip01)
  // 全部元素配对，长度不够以定义的参数补齐  如果配对方长度短，则以*补齐，而被配对方短以-1补齐
  val tuples1: List[(String, Any)] = zip_list.zipAll(pair, "*", -1)
  println(s"集合 全部元素 配对，长度不够参数不齐：${tuples1}")
  val self: List[(String, Int)] = zip_list.zipWithIndex // 元素和自身下标 进行拉练
  println(s"集合元素 和 下标 拉链配对：${self}")
  val unzip: (List[String], List[Int]) = self.unzip // 解除拉链，返回 两个集合
  println(s"解除 拉链：${unzip}")
  println()

  // 不可变 转 可变
  val buffer: mutable.Buffer[Int] = list4.toBuffer
  val toList: List[Int] = buffer.toList

  /** ****** 单词计数 案例 1************ */
  val wc = List("Hello Scala Spark World", "Hello Scala Spark", "Hello Scala", "Hello")
  val list15: List[String] = wc.flatMap(_.split(" ")) // 先将元素 拆分成 单词
  val map_word: Map[String, List[String]] = list15.groupBy(word => word) // 根据拆分的 单词 分组，分组条件 是自身
  val counts: Map[String, Int] = map_word.map(kv => {
    (kv._1, kv._2.length)
  }) // 分组后 ，获取组的长度
  val tuples2: List[(String, Int)] = counts.toList.sortWith(_._2 > _._2).take(3) // 转 list 然后 排序 取 前3
  println(tuples2)

  /** ******** 复杂 的 单词计数 ，句子的 次数也在集合中  *************** */
  val tupleList = List(("Hello Scala Spark World ", 4), ("Hello Scala Spark", 3), ("Hello Scala", 2), ("Hello", 1))
  val word_list: List[String] = tupleList.map(kv => {
    (kv._1.trim + " ") * kv._2 // 去掉多余的 空格符；单词 * 次数
  })
  val tuples3: List[(String, Int)] = word_list.flatMap(_.split(" ")).groupBy(word => word).map(kv => (kv._1, kv._2.length)).toList.sortWith(_._2 > _._2).take(3)
  println(tuples3)

  // 复杂 统计 方式2：
  val tuples4: List[(String, Int)] = tupleList.flatMap(kv => {
    val word = kv._1.split(" ") // 将 单词拆分，每个单词 和 出现的 次数 进行配对
    word.map(str => (str, kv._2)) // 将单词 和 数量 封装
  })
  val tuples5: Map[String, List[(String, Int)]] = tuples4.groupBy(_._1) // 将单词分组
  val tuples6: List[(String, Int)] = tuples5.mapValues(values => { // 遍历 集合 value，求和，返回 (单词，count)
    values.map(_._2).sum
  }).toList.sortBy(_._2).reverse.take(3)
  println(tuples6)


}
