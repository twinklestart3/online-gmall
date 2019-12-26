package com.gtl.gmall.realtime.app

import com.gtl.gmall.common.constant.GmallConstant
import com.gtl.gmall.realtime.util.MyKafkaUtil
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.{Seconds, StreamingContext}

object DauApp {
    def main(args: Array[String]): Unit = {
        // 1.从kafka读数据
        val conf: SparkConf = new SparkConf().setMaster("local[2]").setAppName("DauApp")
        val ssc: StreamingContext = new StreamingContext(conf, Seconds(5))
        val sourceDStream: InputDStream[(String, String)] = MyKafkaUtil.getKafkaStream(ssc, GmallConstant.TOPIC_STARTUP)

        sourceDStream.print(1000)

        // 2.计算DAU

        // 3. 写入hbase(借助phoenixphoenix)

        ssc.start()
        ssc.awaitTermination()
    }
}