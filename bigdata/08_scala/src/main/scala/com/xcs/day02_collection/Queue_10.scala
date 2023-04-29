package com.xcs.day02_collection

import scala.collection.immutable.Queue
import scala.collection.mutable

/**
 * Created with IntelliJ IDEA.
 * Description:bigdata
 *
 * @Author: sky
 *          队列：
 *          可变队列  mutable.Queue
 *          不可变：Queue 默认  就是 mmutable.Queue
 */
object Queue_10 extends App {
  // 可变队列
  //定义可变的队列
  var queue =  mutable.Queue[String]()
  // 入队： 给队列加元素  += 一般用
  queue.enqueue("a")
  queue += "b"
  queue += "c"
  queue += "d"
  queue += "c"
  queue += "c"
  println("定义可变的队列:" + queue)
  // 出队：删除第一个元素
  queue.dequeue()
  println("删除最先进入队列的，及第一个：" + queue)
  //删除第一个满足要求的元素怒
  queue.dequeueFirst(_.equals("c"))
  println("删除第一个满足要求的元素怒：" + queue)
  //删除满足要求的所有元素
  queue.dequeueAll(_.equals("c"))
  println("删除满足要求的所有元素：" + queue)

  //按照下标获取元素
  println("按照下标获取元素：" + queue(0) + "==>" + queue.get(0))
  //如果没有值 就使用默认Some("nothing")
  println(queue.get(20).getOrElse("nothing"))
  println(queue.get(1).getOrElse(Some("nothing")))

  //清空queue
  queue.clear()


  /************不可变 队列************/
  val ints: mutable.Queue[Int] = mutable.Queue(1, 5, 6)
  // 创建 队列
  var queue2 = Queue[Int](1, 2, 3)
  //添加队列元素形成新的队列，一个一个加，如果加元组，会将整个元组当一个元素加入
  // 入队：给队列 添加元素  (1, 2, 3, 0)
  var qu = queue2.enqueue(0)
  println("单个元素加入队列：" + qu)

  //加多个 元素 到 队列（元组） (1, 2, 3, (5,6))
  var qu2 = queue2.enqueue(5, 6)
  println("多个元素加入队列：" + qu2)

  //出队 会去除第一个，返回去除的元素和剩下的元素
  //(1,Queue(2, 3, (5,6)))
  println(qu2.dequeue)

  var num = Queue[Int](1, 2)
  var any = Queue[Any](5, "a")

  //合并队列  union 也可以
  var all = any ++ num
  println("合并队列：" + all)
  println("合并队列：" + any.++(num))
}
