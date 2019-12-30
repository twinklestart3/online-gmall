package com.gtl.gmall.canal.util

import java.util.Properties

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

/**
  * 实现kafka生产者
  */
object MyKafkaSender {
    val props: Properties = new Properties()
    // Kafka服务端的主机名和端口号
    props.setProperty("bootstrap.servers", "hadoop102:9092,hadoop103:9092,hadoop104:9092")
    props.setProperty("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.setProperty("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")

    val producer: KafkaProducer[String, String] = new KafkaProducer[String, String](props)

    def sendToKafka(topic: String, content: String): Unit = {
        producer.send(new ProducerRecord[String, String](topic, content))
    }
}
