package com.atguigu.online.realtime.bean

case class UserAction(
                     uid:String,
                     userName:String,
                     gender:String,
                     level:Int,
                     isVip:Int,
                     os:String,
                     channel:String,
                     netConfig:String,
                     ip:String,
                     phone:String,
                     videoID:Int,
                     videoLength:Int,
                     startVideoTime:Long,
                     endVideoTime:Long,
                     version:String,
                     eventKey:String,
                     var eventTime:String,
                     var region:String
                     ) {

}
