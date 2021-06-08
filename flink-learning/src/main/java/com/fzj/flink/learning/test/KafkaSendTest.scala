package com.fzj.flink.learning.test

import KafkaSendTest.log
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.kafka.common.serialization.{ByteArraySerializer, StringSerializer}
import org.slf4j.{Logger, LoggerFactory}

import java.util.Properties

/**
 * kafka 发送模拟器（用于本地调试）
 */
class KafkaSendTest {

  def produce(): Unit = {
    val brokers = "127.0.0.1:9092"
    val topic = "topic"
    val properties = new Properties()
    properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers)
    properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName)
    properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[ByteArraySerializer].getName)
    val producer = new KafkaProducer[String, Array[Byte]](properties)
    for (_ <- 1 to 100) {
      val message = "message"
      producer.send(new ProducerRecord[String, Array[Byte]](topic, message, message.getBytes))
      log.info("sent message: {}", message)
    }
    producer.close()
  }
}

object KafkaSendTest {

  val log: Logger = LoggerFactory.getLogger(getClass)

  def main(args: Array[String]): Unit = {
    new KafkaSendTest().produce()
  }

}