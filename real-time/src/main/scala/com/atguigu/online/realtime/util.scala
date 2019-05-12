package com.atguigu.online.realtime

import net.ipip.ipdb.BaseStation

object util {
  val db = new BaseStation("D:\\ProgramFiles\\idea\\project\\last-onlinec\\real-time\\src\\main\\resources\\ipipfree.ipdb")
  def main(args: Array[String]): Unit = {

    val strings = db.findInfo("42.86.6.0","CN")

    println(strings.getRegionName)
  }


  def getRegionName(ip:String):String={
    val regionName = db.findInfo(ip,"CN").getRegionName
    regionName
  }
}
