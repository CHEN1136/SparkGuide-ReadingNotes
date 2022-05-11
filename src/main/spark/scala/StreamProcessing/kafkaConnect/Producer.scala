package scala.StreamProcessing.kafkaConnect

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}

import java.util.Properties
import scala.util.Random

object Producer {
  def main(args: Array[String]): Unit = {
    //从运行时参数读入topic
    val topic = "myTopic"

    //从运行时参数读入brokers
    val brokers = "localhost:9092"

    //设置一个随机数
    val random = new Random()

    //配置参数
    val prop = new Properties()

    //配置brokers
    prop.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,brokers)

    //序列化类型
    prop.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
      "org.apache.kafka.common.serialization.StringSerializer")
    prop.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
      "org.apache.kafka.common.serialization.StringSerializer")

    //建立Kafka连接
    val kafkaProducer = new KafkaProducer[String,String](prop)

    val t = System.currentTimeMillis()

    //模拟用户名地址数据
    val nameAddrs = Map("bob"->"shanghai#200000",
      "amy"->"beijing#100000",
      "alice"->"shanghai#200000",
      "tom"->"beijing#100000",
      "lulu"->"hangzhou#310000",
      "nick"->"shanghai#200000")

    //模拟用户名电话数据
    val namePhones = Map(
      "bob"->"15700079421",
      "amy"->"18700079458",
      "alice"->"17730079427",
      "lulu"->"18800074423",
      "nick"->"14400033426"
    )

    for(nameAddr <- nameAddrs){
      val data = new ProducerRecord[String,String](topic,nameAddr._1,
        s"${nameAddr._1}\t${nameAddr._2}\t0")
      //数据写入Kafka
      kafkaProducer.send(data)

      if(random.nextInt(100)<50) Thread.sleep(random.nextInt(10))
    }

    for(namePhone<- namePhones){
      val data = new ProducerRecord[String,String](topic,namePhone._1,
        s"${namePhone._1}\t${namePhone._2}\t1")
      kafkaProducer.send(data)
      if(random.nextInt(100)<50) Thread.sleep(random.nextInt(10))
    }
    System.out.println("Send peer second:"+(nameAddrs.size+namePhones.size)*1000/
      (System.currentTimeMillis()-t))
    //Send peer second:16
    //生成模拟数据 （name，addr，type：0）
    kafkaProducer.close()
  }
}
