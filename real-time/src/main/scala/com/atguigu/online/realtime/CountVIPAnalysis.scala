package com.atguigu.online.realtime

import java.sql.Date
import java.text.SimpleDateFormat
import java.util.Properties
import com.atguigu.online.realtime.bean.UserAction
import kafka.common.TopicAndPartition
import kafka.message.MessageAndMetadata
import kafka.serializer.StringDecoder
import org.apache.spark.streaming.kafka.{HasOffsetRanges, KafkaUtils, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.SparkConf
import scalikejdbc.{ConnectionPool, DB}
import scalikejdbc._


object CountVIPAnalysis {

  //提出公共变量,转换算子共用
  val sdf = new SimpleDateFormat("yyyy-MM-dd")

  //从配置文件中读取信息
  val prop = new Properties()
  prop.load(this.getClass.getClassLoader.getResourceAsStream("config.properties"))

  def getVipIncrementByCountry(checkPoint: String): StreamingContext = {

    //设置批处理时间
    val interval = prop.getProperty("IntervalTime").toLong
    //获取kafka相关参数
    val brokers = prop.getProperty("kafka.brokers")
    val topic = prop.getProperty("kafka.topic")

    val topicsSet = topic.split(",").toSet

    val sparkConf = new SparkConf().set("spark.streaming.stopGracefullyOnShutdown","true").setAppName(this.getClass.getSimpleName).setMaster("local[*]")
    val ssc = new StreamingContext(sparkConf, Seconds(interval))

    val kafkaParams = Map[String, String]("metadata.broker.list" -> brokers,  "auto.offset.reset" -> "smallest")

    // 获取offset
    val fromOffsets = DB.readOnly { implicit session => sql"select topic, part_id, offset from topic_offset".
      map { r =>
        TopicAndPartition(r.string(1), r.int(2)) -> r.long(3)
      }.list.apply().toMap
    }

    val messageHandler = (mmd : MessageAndMetadata[String, String]) => (mmd.topic, mmd.message())
    var offsetRanges : Array[OffsetRange] = Array.empty[OffsetRange]

    // 获取Dstream
    val  message = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder, (String, String)](ssc, kafkaParams, fromOffsets, messageHandler)

    //开启检查点
    ssc.checkpoint(checkPoint)
    message.checkpoint(Seconds(interval*10))

    //业务计算
    val resultDstream = message.transform(rdd => {
      offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      rdd
    }).map(_._2).filter(checkEventValid(_)).map(maskPhone(_)).map(repairUserName(_)).map(str => {
      val strarr = str.split("\t")
      val action = new UserAction(strarr(0), strarr(1), strarr(2), strarr(3).toInt, strarr(4).toInt, strarr(5), strarr(6), strarr(7), strarr(8), strarr(9), strarr(10).toInt, strarr(11).toInt, strarr(12).toLong, strarr(13).toLong, strarr(14), strarr(15), strarr(16), null)
      action
    }).map(ac => {
      ac.region = util.getRegionName(ac.ip)
      ac
    }).map(ac => {
      val date = sdf.format(new Date(ac.eventTime.toLong * 1000))
      ac.eventTime = date
      ac
    }).map(ac => (ac.eventTime + "_" + ac.region, 1)).updateStateByKey((seq, opt: Option[Int]) => {
      //将相同的K所形成的序列进行聚合
      var count = seq.sum + opt.getOrElse(0)
      Option(count)
    })

    //将结果写入mysql
    resultDstream.foreachRDD(rdd=>{
      rdd.foreachPartition(iter=>{
        //开启事务
        DB.localTx{implicit session=>
          iter.foreach(t=>{
            val str = t._1.split("_")
            val dt = str(0)
            val province = str(1)
            val cnt = t._2

            //持久化到mysql中
            sql"""replace into vip_increment_analysis(province,cnt,dt) values (${province},${cnt},${dt})""".executeUpdate().apply()
          })

          offsetRanges.foreach(t=>{
            sql"""update topic_offset set offset = ${t.untilOffset} where topic = ${t.topic} and part_id = ${t.partition}""".update.apply()
          })
        }
      })
    })
    ssc
  }

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

    //通过getOrCreate方式实现从Driver端的失败恢复
    val checkPoint = "checkPoint"

    val ssc = StreamingContext.getOrCreate(checkPoint, () => {
      getVipIncrementByCountry(checkPoint)
    })
    ssc.start()
    ssc.awaitTermination()
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
