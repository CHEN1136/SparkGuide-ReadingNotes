package scala.StreamProcessing.kafkaConnect

import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.{Seconds, StreamingContext}

object Consumer extends  App {
  val conf = new SparkConf().
    setMaster("local")
    .setAppName("kafkaSparkStreaming")
    .set("spark.streaming.kafka.MaxRatePerPartition","3")
    .set("spark.local.dir","tmp")
  //创建上下文，2s为批处理间隔
  val ssc = new StreamingContext(conf,Seconds(2))

  //设置日志级别
  ssc.sparkContext.setLogLevel("WARN")

  //配置kafka参数，根据broker和topic创建连接Kafka 直接连接 direct kafka
  val KafkaParams = Map[String,Object](
    //brokers地址
    "bootstrap.servers"->"localhost:9092",
    //序列化类型
    "key.deserializer"->classOf[StringDeserializer],
    "value.deserializer" -> classOf[StringDeserializer],
    "group.id" -> "MyGroupId",
    //设置手动提交消费者offset
    "enable.auto.commit" -> (false: java.lang.Boolean)//默认是true
  )

  //获取KafkaDStream
  val kafkaDirectStream = KafkaUtils.createDirectStream[String,String](ssc,
    //test0307为topic
    PreferConsistent,Subscribe[String,String](List("myTopic"),KafkaParams))


  //根据得到的kafak信息，切分得到用户电话DStream
  val nameAddrStream = kafkaDirectStream.map(_.value()).filter(record=>{
    val tokens: Array[String] = record.split("\t")
    tokens(2).toInt==0
  }).map(record=>{
    val tokens = record.split("\t")
    (tokens(0),tokens(1))
  })

  val namePhoneStream = kafkaDirectStream.map(_.value()).filter(
    record=>{
      val tokens = record.split("\t")
      tokens(2).toInt == 1
    }
  ).map(record=>{
    val tokens = record.split("\t")
    (tokens(0),tokens(1))
  })

  //以用户名为key，将地址电话配对在一起，并产生固定格式的地址电话信息
  val nameAddrPhoneStream = nameAddrStream.join(namePhoneStream).map(
    record=>{
      s"姓名：${record._1},地址：${record._2._1},邮编：${record._2._2}"
    }
  )
  //打印输出
  nameAddrPhoneStream.print()

  //开始计算
  ssc.start()

  ssc.awaitTermination()

}
