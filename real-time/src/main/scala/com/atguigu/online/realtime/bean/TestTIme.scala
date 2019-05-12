package com.atguigu.online.realtime.bean

import java.text.SimpleDateFormat
import java.util.Date

object TestTIme {
  def main(args: Array[String]): Unit = {
    val sdf = new SimpleDateFormat("yyyy-MM-dd")

    val str = sdf.format(new Date("1554652800".toLong *1000))

    println(str)
  }
}
