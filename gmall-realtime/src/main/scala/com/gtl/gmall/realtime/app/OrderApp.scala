package com.gtl.gmall.realtime.app

import com.alibaba.fastjson.JSON
import com.gtl.gmall.common.constant.GmallConstant
import com.gtl.gmall.realtime.bean.OrderInfo
import com.gtl.gmall.realtime.util.MyKafkaUtil
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object OrderApp {
    def main(args: Array[String]): Unit = {
        val conf: SparkConf = new SparkConf().setMaster("local[2]").setAppName("OrderApp")
        val ssc: StreamingContext = new StreamingContext(conf, Seconds(5))

        // 1. 从kafka消费数据
        val OrderInfoInputDStream: InputDStream[(String, String)] = MyKafkaUtil.getKafkaStream(ssc, GmallConstant.TOPIC_ORDER)

        // 2. 处理数据
        val orderInfoDStream: DStream[OrderInfo] = OrderInfoInputDStream.map{
            case (_, orderInfoJson) =>
                // 转换数据格式
                val orderInfo: OrderInfo = JSON.parseObject(orderInfoJson, classOf[OrderInfo])
                // 数据脱敏
                orderInfo.consignee.substring(0, 1) + "**"
                orderInfo.consignee_tel.replaceAll("(\\d{3})(\\d{4})(\\d{3})", "$1****$3")
                orderInfo
        }
        orderInfoDStream.print(1000)

        // 3. 将数据写入到HBase(phoenix)
        import org.apache.phoenix.spark._
        orderInfoDStream.foreachRDD(rdd => {
            rdd.saveToPhoenix("GMALL_ORDER_INFO",
                Seq("ID", "PROVINCE_ID", "CONSIGNEE", "ORDER_COMMENT", "CONSIGNEE_TEL", "ORDER_STATUS", "PAYMENT_WAY", "USER_ID", "IMG_URL", "TOTAL_AMOUNT", "EXPIRE_TIME", "DELIVERY_ADDRESS", "CREATE_TIME", "OPERATE_TIME", "TRACKING_NO", "PARENT_ORDER_ID", "OUT_TRADE_NO", "TRADE_BODY", "CREATE_DATE", "CREATE_HOUR"),
                zkUrl = Some("hadoop102,hadoop103,hadoop104:2181")
            )
        })

        ssc.start()
        ssc.awaitTermination()
    }
}
