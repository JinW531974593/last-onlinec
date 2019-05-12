package com.atguigu.online.realtime

import java.sql.DriverManager
import java.text.SimpleDateFormat
import java.util.Date

import com.atguigu.online.realtime.bean.UserAction
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka.{HasOffsetRanges, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object VIPCount {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("VIPCount").setMaster("local[*]")

    val streamingContext = new StreamingContext(sparkConf,Seconds(5))
     val kafkaParam = Map(
        "bootstrap.servers" -> "hadoop102:9092",
        "auto.offset.reset" -> "smallest"
      )

    var offsetRanges:Array[OffsetRange] = Array.empty[OffsetRange]
    implicit val groupName = "kafka_consumer_online"
    val kafkaDStream = kafkaDirectDemo.getKafkaDirectStream(streamingContext,kafkaParam,"online").transform(rdd=>{
      offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
//      kafkaDirectDemo.saveOffsets(offsetRanges)
      rdd
    })

    kafkaDirectDemo.saveOffsets(offsetRanges)

    println(offsetRanges.length)
    val recordDStream = kafkaDStream.map(t=>t._2)



    //过滤清洗数据
    val etlDStream = recordDStream.filter(checkEventValid(_)).map(maskPhone(_)).map(repairUserName(_))

    //将数据封装为对象，以方便后续操作
    val actionDStream = etlDStream.map(str => {
      val strarr = str.split("\t")
      val action = new UserAction(strarr(0), strarr(1), strarr(2), strarr(3).toInt, strarr(4).toInt, strarr(5), strarr(6), strarr(7), strarr(8), strarr(9), strarr(10).toInt, strarr(11).toInt, strarr(12).toLong, strarr(13).toLong, strarr(14), strarr(15), strarr(16), null)
      action
    })

    //设置检查点目录
    streamingContext.sparkContext.setCheckpointDir("D:\\ProgramFiles\\idea\\project\\last-onlinec\\real-time\\checkpoint")
    //获取省份字段
    val regionDStream = actionDStream.map(ac => {
      val region = util.getRegionName(ac.ip)
      ac.region = region
      ac
    })

    //过滤其他用户,只留下新增用户
    val filterDStream = regionDStream.filter(t=>t.eventKey=="completeOrder")

    //过滤当天多次充值的用户

    val UserVipDStream = filterDStream.transform(rdd => {
      val value = rdd.groupBy(_.uid)
      val singleRDD = value.mapValues(iter => {
        val actions = iter.toList
        actions(0)
      })
      val singleActionRDD = singleRDD.map(t => t._2)
      singleActionRDD
    })

    //格式化成为vip的时间
    val sdf = new SimpleDateFormat("yyyy-MM-dd")

    val formatTimeDStream = UserVipDStream.map(ac => {
      val date = sdf.format(new Date(ac.eventTime.toLong * 1000))
      ac.eventTime = date
      ac
    })
    //按时间 和 地区 对用户进行分类
    //转换格式为(time_region,1)

    val kvDStream = formatTimeDStream.map(ac=>(ac.eventTime+"_"+ac.region,1))

    val result = kvDStream.updateStateByKey((seq, opt: Option[Int]) => {
      //将相同的K所形成的序列进行聚合
      var count = seq.sum + opt.getOrElse(0)
      Option(count)
    })

    /**
      * jdbc.driver.class=com.mysql.jdbc.Driver
      * jdbc.url=jdbc:mysql://hadoop102:3306/sparkmall?useUnicode=true&characterEncoding=utf8&rewriteBatchedStatements=true
      * jdbc.user=root
      * jdbc.password=123456
      */
    val driver = "com.mysql.jdbc.Driver"
    val url =  "jdbc:mysql://hadoop102:3306/online?useUnicode=true&characterEncoding=utf8&rewriteBatchedStatements=true"

    val user = "root"
    val password = "123456"

    val sql = "replace into vip_increment_analysis values(?,?,?)"
    result.foreachRDD(rdd=>{
      rdd.foreachPartition(iter=>{
        Class.forName(driver)
        val connection = DriverManager.getConnection(url,user,password)
        val ps = connection.prepareStatement(sql)
        for((key,count) <- iter){
          val str = key.split("_")
          val date = str(0)
          val region = str(1)
          ps.setString(1,region)
          ps.setInt(2,count)
          ps.setString(3,date)
          ps.executeUpdate()
        }
        ps.close()
        connection.close()
      })
    })


    streamingContext.start()
    streamingContext.awaitTermination()
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
