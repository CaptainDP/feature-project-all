package com.captain.bigdata.taichi.demo.app

import com.captain.bigdata.taichi.TaichiApp

/**
  * DemoTestFile
  *
  * @author <a href=mailto:captain_cc_2008@163.com>CaptainDP</a>
  * @date 2018/2/1 16:26
  * @func
  */
object DemoTestFile {

  def main(args: Array[String]): Unit = {

    val args2 = Array[String]("-d", "20190101", "-cf", "./conf/test_file.json")
    TaichiApp.main(args2)

  }


}
