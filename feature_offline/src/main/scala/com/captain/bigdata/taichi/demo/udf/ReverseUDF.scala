package com.captain.bigdata.taichi.demo.udf

import org.apache.hadoop.hive.ql.exec.UDF

/**
  * ReverseUDF
  *
  * @author <a href=mailto:captain_cc_2008@163.com>CaptainDP</a>
  * @date 2017/12/4 10:44
  * @func String flip function
  */
class ReverseUDF extends UDF {

  def evaluate(record: String): String = {

    var result: String = ""
    if (record != null) {
      result = record.reverse
    }

    result
  }

}

object ReverseUDF {

  def main(args: Array[String]): Unit = {

    val udf = new ReverseUDF
    val result = udf.evaluate("123456")
    println(result)

  }

}
