package com.atguigu.online.realtime
import java.sql.Date
import java.text.SimpleDateFormat
import java.util.Properties

import kafka.common.TopicAndPartition
import kafka.message.MessageAndMetadata
import kafka.serializer.StringDecoder
import net.ipip.ipdb.City
import org.apache.spark.streaming.kafka.{HasOffsetRanges, KafkaUtils, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.SparkConf
import scalikejdbc.{ConnectionPool, DB}
import scalikejdbc._

/**
  * 按地区分组统计每日新增VIP数量
  */
object VipIncrementAnalysis {
  // 提取出公共变量，转换算子共用
  val sdf = new SimpleDateFormat("yyyy-MM-dd")

  // 从properties文件里获取各种参数
  val prop = new Properties()
  prop.load(this.getClass.getClassLoader().getResourceAsStream("VipIncrementAnalysis.properties"))

  // 使用静态ip资源库
  val ipdb = new City(this.getClass().getClassLoader().getResource("ipipfree.ipdb").getPath())

  def main(args : Array[String]): Unit ={
    // 参数检测
    if(args.length != 1){
      println("Usage:Please input checkpointPath")
      System.exit(1)
    }

    // 通过传入参数设置检查点
    val checkPoint = args(0)

    // 获取jdbc相关参数
    val driver = prop.getProperty("jdbcDriver")
    val jdbcUrl =  prop.getProperty("jdbcUrl")
    val jdbcUser = prop.getProperty("jdbcUser")
    val jdbcPassword = prop.getProperty("jdbcPassword")

    // 设置jdbc
    Class.forName(driver)
    // 设置连接池
    ConnectionPool.singleton(jdbcUrl, jdbcUser, jdbcPassword)

    // 通过getOrCreate方式可以实现从Driver端失败恢复
    val ssc = StreamingContext.getOrCreate(checkPoint,
      () => {getVipIncrementByCountry(checkPoint)}
    )

    // 启动流计算
    ssc.start()
    ssc.awaitTermination()
  }

  // 通过地区统计vip新增数量
  def getVipIncrementByCountry(checkPoint : String): StreamingContext ={
    // 定义update函数
    val updateFunc = (values: Seq[Int], state: Option[Int]) => {
      // 本批次value求合
      val currentCount = values.sum

      val previousCount = state.getOrElse(0)
      Some(currentCount + previousCount)
    }

    // 设置批处理间隔
    val processingInterval = prop.getProperty("processingInterval").toLong

    // 获取kafka相关参数
    val brokers = prop.getProperty("brokers")
    val topic = prop.getProperty("topic")

    // Create direct kafka stream with brokers and topics
    val topicsSet = topic.split(",").toSet

    val sparkConf = new SparkConf().set("spark.streaming.stopGracefullyOnShutdown","true").setAppName(this.getClass.getSimpleName).setMaster("local[*]")
    val ssc = new StreamingContext(sparkConf, Seconds(processingInterval))

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
    val  messages = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder, (String, String)](ssc, kafkaParams, fromOffsets, messageHandler)

    // 开启检查点
    ssc.checkpoint(checkPoint)
    messages.checkpoint(Seconds(processingInterval * 10))

    // 业务计算
    messages.transform{rdd =>
      offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      rdd
    }.filter{ msg =>
      // 过滤非完成订单的数据，且验证数据合法性
      filterCompleteOrderData(msg)
    }.map{ msg =>
      // 数据转换，返回((2019-04-03,北京),1)格式的数据
      getCountryAndDate(msg)
    }.updateStateByKey[Int](updateFunc).filter{ state =>
      // 只保留最近2天的状态，而不只保存1天的状态是考虑跨天的情况
      filter2DaysBeforeState(state)
    }.foreachRDD(rdd=> {
      rdd.foreachPartition(partiton => {
        // 开启事务
        DB.localTx { implicit session =>
          partiton.foreach(msg => {
            val dt = msg._1._1
            val province = msg._1._2
            val cnt = msg._2.toLong

            // 统计结果持久化到Mysql中
            sql"""replace into vip_increment_analysis(province,cnt,dt) values (${province},${cnt},${dt})""".executeUpdate().apply()
            println(msg)
          })

          for (o <- offsetRanges) {
            println(o.topic,o.partition,o.fromOffset,o.untilOffset)
            // 保存offset
            sql"""update topic_offset set offset = ${o.untilOffset} where topic = ${o.topic} and part_id = ${o.partition}""".update.apply()
          }
        }
      })
    })
    ssc
  }

  /**
    * 只保留最近2天的状态，而不只保存1天的状态是考虑跨天的情况
    */
  def filter2DaysBeforeState(state : ((String,String),Int)): Boolean ={
    // 获取状态值对应的日期，并转换为13位的长整型时间缀
    val day = state._1._1
    val eventTime = sdf.parse(day).getTime
    // 获取当前系统时间缀
    val currentTime = System.currentTimeMillis()
    // 两者比较，保留两天内的
    if(currentTime - eventTime < 172800000){
      false
    }else{
      true
    }
  }

  /**
    * 过滤非完成订单的数据，且验证数据合法性
    */
  def filterCompleteOrderData(msg : (String,String)): Boolean ={
    val fields = msg._2.split("\t")
    // 切分后长度不为17，代表数据不合法
    if(fields.length == 17){
      val eventType = msg._2.split("\t")(15)
      "completeOrder".equals(eventType)
    }else{
      false
    }
  }

  /**
    * 数据转换，返回((2019-04-03,北京),1)格式的数据
    * @param msg
    * @return
    */
  def getCountryAndDate(msg : (String,String)): ((String,String),Int) ={
    val fields = msg._2.split("\t")
    // 获取ip地址
    val ip = fields(8)
    // 获取事件时间
    val eventTime = fields(16).toLong

    // 根据日志中的eventTime获取对应的日期
    val date = new Date(eventTime * 1000)
    val eventDay = sdf.format(date)

    // 根据IP获取省份信息
    var regionName = "未知"
    val info = ipdb.findInfo(ip,"CN")
    if(info != null){
      regionName = info.getRegionName()
    }

    ((eventDay,regionName),1)
  }
}
