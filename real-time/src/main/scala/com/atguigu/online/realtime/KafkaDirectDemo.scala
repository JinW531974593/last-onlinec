package com.atguigu.online.realtime

import kafka.common.TopicAndPartition
import kafka.message.MessageAndMetadata
import kafka.serializer.StringDecoder
import org.apache.curator.framework.{CuratorFramework, CuratorFrameworkFactory}
import org.apache.curator.retry.ExponentialBackoffRetry
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka.{KafkaUtils, OffsetRange}

import scala.collection.JavaConversions._

object kafkaDirectDemo extends Serializable {
  implicit val groupName = "kafka_consumer_online"
  val client = {
    val client: CuratorFramework = CuratorFrameworkFactory.builder().connectString("hadoop102:2181").retryPolicy(new ExponentialBackoffRetry(1000, 3)).namespace("online").build()
    client.start()
    client
  }
  // kafka 在zk的路径
  val kafkaPath = "/onion_realtime"


  /**
    * 从ZK中获取offset
    */
  def getOffsetFromZk(topic:String)(implicit groupName:String)={
    //获取zk中相应主题的offset作为消费数据的起点
    var fromOffsets:Map[TopicAndPartition,Long] = Map()

    val zkTopicPath = s"${kafkaPath}/${groupName}/${topic}"

    println(zkTopicPath)

    //检查zk中的路径是否存在
    if (client.checkExists().forPath(zkTopicPath) == null){

      client.create().creatingParentsIfNeeded().forPath(zkTopicPath)
    }

    val strings = client.getChildren.forPath(zkTopicPath)
    println(strings)
    //获取每个分区的offset，
    for(p<-client.getChildren.forPath(zkTopicPath)) {
      val date = client.getData.forPath(s"${zkTopicPath}/$p")

      //将字节数组转换成Long
      val offset = java.lang.Long.valueOf(new String(date)).toLong

      println(s"$topic-$groupName is offset:$offset")

      fromOffsets += (TopicAndPartition(topic, Integer.parseInt(p)) -> offset)
    }

    //如果map为空，代表zk中没有保存过offset，此时设标记位为0，如果非空，则代表在zk中保存过offset，设标记位为1
    if (fromOffsets.isEmpty) {
      (fromOffsets,0)
    }else{
      (fromOffsets,1)
    }
  }

  /**
    * 保存offset到ZK
    * @param offsetsRanges
    * @param groupName
    */
  def saveOffsets(offsetsRanges:Array[OffsetRange])(implicit groupName:String): Unit ={
    for (o <- offsetsRanges){
      //保存offset到zk
      val zkPath=s"${kafkaPath}/${groupName}/${o.topic}/${o.partition}"

      // 确保zk中的路径一定存在
      if (client.checkExists().forPath(zkPath) == null) {
        client.create().creatingParentsIfNeeded().forPath(zkPath)
      }
      client.setData().forPath(zkPath,o.untilOffset.toString.getBytes())
    }
  }

  def getKafkaDirectStream(ssc:StreamingContext,kafkaparams:Map[String,String],topic:String)(implicit groupName:String) = {
    //获取kafka的offset与标记位置
    val (fromOffsets, flag): (Map[TopicAndPartition, Long], Int) = getOffsetFromZk(topic)

    var kafkaStream: InputDStream[(String, String)] = null

    if (flag == 1) {
      //这个会将kafka的消息进行transform，最终kafka的数据都会变成(topic_name,message)
      val messageHandler = (mmd:MessageAndMetadata[String,String])=>(mmd.topic,mmd.message())

      kafkaStream = KafkaUtils.createDirectStream[String,String,StringDecoder,StringDecoder,(String,String)](ssc,kafkaparams,fromOffsets,messageHandler)
    }else{
      //如果未保存，根据kafkaParam的配置使用最新或者最旧的offset(根据业务需求)
      kafkaStream = KafkaUtils.createDirectStream[String,String,StringDecoder,StringDecoder](ssc,kafkaparams,topic.split(",").toSet)
    }

    kafkaStream
  }
}
