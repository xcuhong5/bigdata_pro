package com.xcs.day01_basics

import java.io.{BufferedReader, File, PrintWriter}
import scala.io.Source


/**
 * Created with IntelliJ IDEA.
 * Description:bigdata
 *
 * @Author: sky
 *          3. 读写 文件
 */
object ReadFile_03 {
  def main(args: Array[String]): Unit = {
    /** **** 1. 从文件中读取 数据 ,工程 是聚合工程 ，项目名/相对路径 **** */
    val source = Source.fromFile("08_scala/src/main/resources/readfile03.txt")
    // 按行 读
    val lines: Iterator[String] = source.getLines()
    for (line <- lines) {
      println(s"按行 读取文件内容：${line}")
    }
    source.close()
    println()

    // 一个一个 字符的 读取
    val source2 = Source.fromFile("08_scala/src/main/resources/readfile03.txt")
    val buffered: BufferedIterator[Char] = source2.buffered
    while (buffered.hasNext) {
      println(s"按照 字符读取：${buffered.next()}")
    }
    source2.close() // 关闭 流
    println()

    /** ****** 2. 网络文件读写****** */
    val netRead = Source.fromURL("https://www.baidu.com/")
    val iterator: Iterator[String] = netRead.getLines()
    while (iterator.hasNext) {
      println("读取网站html：" + iterator.next())
    }
    netRead.close()


    /********** 3.写入 数据 到 文件****************/
    val printWriter = new PrintWriter(new File("08_scala/src/main/resources/writefile03.txt"))
    printWriter.write("你好，你是谁？")
    printWriter.write("你是谁？")
    printWriter.close()


    /********** 4. 输入输出 对接 *********/
    val out = new PrintWriter("08_scala/src/main/resources/writefile04.txt")
    val net = Source.fromURL("https://v.qq.com/")
    val iter: Iterator[String] = net.getLines()
    while (iter.hasNext) {
      out.print(iter.next())
    }
    out.close()
    net.close()

  }
}
