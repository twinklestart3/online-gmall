package com.gtl.gmall.realtime.util

import kafka.serializer.StringDecoder
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka.KafkaUtils

object MyKafkaUtil {
    val kafkaParam: Map[String, String] = Map[String, String](
        ConsumerConfig.GROUP_ID_CONFIG -> PropertiesUtil.getProperty("config.properties", "kafka.group"),
        ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> PropertiesUtil.getProperty("config.properties", "kafka.broker.list")
    )

    def getKafkaStream(ssc: StreamingContext, topic: String): InputDStream[(String, String)] = {
        KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParam, Set(topic))
    }
}
