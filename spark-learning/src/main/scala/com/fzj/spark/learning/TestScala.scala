package com.fzj.spark.learning

import com.fzj.spark.learning.cases.Domain

object TestScala {
  def main(args: Array[String]): Unit = {
    //模式匹配
    val domain1  = Domain(1, 111, "name1", "field1")
    val domain2  = Domain(2, 222, "name2", "field1")
    val domain3  = Domain(3, 333, "name3", "field1")
    for (domain <- List(domain1, domain2, domain3)) {
      domain match {
        case Domain(1, 111, "name1", "field1") => println(domain.name)
        case Domain(2, 222, "name2", "field1") => println(domain.field)
        case Domain(_, id2, name, age) =>
          println(domain.ID, domain.id, domain.name)
      }
    }

    printf("hello world")
  }

  //函数， 闭包，一个实现了 trait 的对象
  var testFunction: Int => Int = (i: Int) => i * 10
  //方法
  def testMethod(i: Int): Int = i * 11
}
