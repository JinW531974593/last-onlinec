package com.atguigu.online.realtime

import java.util.Properties

import com.atguigu.online.realtime.bean.UserAction
import kafka.common.TopicAndPartition
import kafka.message.MessageAndMetadata
import kafka.serializer.StringDecoder
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka.{HasOffsetRanges, KafkaUtils, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import scalikejdbc._
import scalikejdbc.{ConnectionPool, DB}


object PotentialVipTransform {
    //加载配置文件
    val prop = new Properties()
    prop.load(this.getClass.getClassLoader.getResourceAsStream("config.properties"))

  def main(args: Array[String]): Unit = {
    //获取jdbc的相关参数
    val driver = prop.getProperty("jdbc.driver.class")
    val url = prop.getProperty("jdbc.url")
    val user = prop.getProperty("jdbc.user")
    val password = prop.getProperty("jdbc.password")

    //加载驱动
    Class.forName(driver)

    //设置连接池
    ConnectionPool.singleton(url,user,password)

    val ssc = getVipConsumer()

    //启动流计算
    ssc.start()
    ssc.awaitTermination()
  }


  def getVipConsumer():StreamingContext={

    //获取批处理时间
    val intervalTime = prop.getProperty("IntervalTime").toLong
    //获取kafka相关参数

    val brokers = prop.getProperty("kafka.brokers")
    val topic = prop.getProperty("kafka.topic")


    //创建kafka direct stream with brokers and topics

    val topicSet = topic.split(",").toSet

    val sparkConf = new SparkConf().set("spark.streaming.stopGracefullyOnShutdown", "true").setAppName(this.getClass.getSimpleName).setMaster("local[*]")

    val ssc = new StreamingContext(sparkConf,Seconds(intervalTime))

    val kafkaParams = Map[String, String]("metadata.broker.list" -> brokers,  "auto.offset.reset" -> "smallest")

    //获取offset

    val fromOffsets = DB.readOnly{implicit session=>sql"select topic,part_id,offset from unpayment_topic_offset".map{r=>
      TopicAndPartition(r.string(1),r.int(2))->r.long(3)
      }.list().apply().toMap
    }

    val messageHandler = (mmd:MessageAndMetadata[String,String]) =>(mmd.topic,mmd.message())
    //初始化offsetRanges
    var offsetRanges:Array[OffsetRange] = Array.empty[OffsetRange]


    //获取DStream
    val kafkaDStream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder, (String, String)](ssc, kafkaParams, fromOffsets, messageHandler)


    //获取偏移量以方便后续保存到mysql中
    kafkaDStream.transform(rdd=>{
      offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      println(offsetRanges.length)
      rdd
    })

    //处理业务逻辑

    //1.对数据进行清洗，并封装为对象，以方便后续操作
    val etlDStream = kafkaDStream.map(_._2).filter(checkEventValid(_)).map(maskPhone(_)).map(repairUserName(_)).map(str => {
      val strarr = str.split("\t")
      val action = new UserAction(strarr(0), strarr(1), strarr(2), strarr(3).toInt, strarr(4).toInt, strarr(5), strarr(6), strarr(7), strarr(8), strarr(9), strarr(10).toInt, strarr(11).toInt, strarr(12).toLong, strarr(13).toLong, strarr(14), strarr(15), strarr(16), null)
      action
    })



    val windowDStream = etlDStream.window(Seconds(intervalTime*4),Seconds(intervalTime))

    //抓取进入支付订单的用户,并转化为(user,1)，然后进行聚合,过去剩下为异常用户
    val userCenterCount3 = windowDStream.filter(_.eventKey=="enterOrderPage").map(ac=>(ac,1)).reduceByKey(_+_).filter(t=>t._2>=3)

    //过滤掉为vip的用户
    val newUserDstream = userCenterCount3.map(t => t._1.uid).filter(uid => {
      val uservip = DB.readOnly{implicit session=>sql"select uid from vip_user ".map(r=>r.string(1)).list().apply()
      }
      !uservip.contains(uid)
    })

    //将异常用户持久化到mysql中
    newUserDstream.foreachRDD(rdd=>{
     rdd.foreachPartition{iter=>
       //开启事务
       DB.localTx{implicit session=>
         iter.foreach(t=>{
           val uid = t

           sql"""insert into unpayment_record values (${uid})""".executeUpdate().apply()
         })


         for (o <- offsetRanges) {
           println(o.topic,o.partition,o.fromOffset,o.untilOffset)
           // 保存offset
           sql"""update unpayment_topic_offset set offset = ${o.untilOffset} where topic = ${o.topic} and part_id = ${o.partition}""".executeUpdate().apply()
         }
       }
     }
    })

    ssc
  }




  /**
    * 验证是否是有效数据
    */
  def checkEventValid(str:String)={
    val strArr = str.split("\t")
    strArr.length == 17
  }

  /**
    * 对手机号进行脱敏处理，对区号字段
    * @param data
    */
  def maskPhone(data:String)= {
    val strArr = data.split("\t")
    //StringBuilder的效率更高
    val newPhone = new StringBuilder()
    val phone = strArr(9)

    if (!"".equals(phone) && phone!=null) {
      newPhone.append(phone.substring(0,3)).append("xxxx").append(phone.substring(7))

      strArr(9) = newPhone.toString()
    }

    strArr.mkString("\t")
  }

  /**
    * 修复用户的用户名的错误输入，防止存入hdfs时出现换行
    *
    */

  def repairUserName(str:String)= {

    val strings = str.split("\t")
    val username = strings(1)

    //判断用户名是否是有效数据
    if (!"".equals(username)&&username!=null) {
      val newUserName = username.replace("\n","")
      strings(1) = newUserName
    }

    strings.mkString("\t")
  }
}
