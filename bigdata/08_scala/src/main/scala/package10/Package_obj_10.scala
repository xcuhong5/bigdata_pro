/**
 * Created with IntelliJ IDEA.
 * Description:bigdata
 *
 * @Author: sky
 *          包对象
 *          父包 访问 子包需要导包；子包访问 父包不用导
 */
package com {
  // 父包 访问 子包 需要 导包

  import com.two.scala.Inner

  object Out {
    val o = "out"

    def main(args: Array[String]): Unit = {
      println("外层访问 iner：" + Inner.i)
    }

  }
  package two {
    package scala {
      object Inner {
        val i = "iner"

        def main(args: Array[String]): Unit = {
          println("内层访问 Out：" + Out.o)
        }
      }
    }

  }

}