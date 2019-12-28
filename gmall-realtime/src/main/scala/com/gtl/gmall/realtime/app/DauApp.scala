package com.gtl.gmall.realtime.app

import java.text.SimpleDateFormat
import java.util
import java.util.Date

import com.alibaba.fastjson.JSON
import com.gtl.gmall.common.constant.GmallConstant
import com.gtl.gmall.realtime.bean.StartupLog
import com.gtl.gmall.realtime.util.{MyKafkaUtil, RedisUtil}
import org.apache.spark.SparkConf
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import redis.clients.jedis.Jedis

object DauApp {
    def main(args: Array[String]): Unit = {
        // 1.从kafka读数据(启动日志)
        val conf: SparkConf = new SparkConf().setMaster("local[2]").setAppName("DauApp")
        val ssc: StreamingContext = new StreamingContext(conf, Seconds(5))
        val sourceDStream: InputDStream[(String, String)] = MyKafkaUtil.getKafkaStream(ssc, GmallConstant.TOPIC_STARTUP)

        //sourceDStream.print(1000)

        // 2.封装数据
        val startupLogDStream: DStream[StartupLog] = sourceDStream.map {
            case (_, log) => JSON.parseObject(log, classOf[StartupLog])
        }
        //startupLogDStream.print(1000)

        // 3.保存到redis：不经处理直接保存到redis
        // 缺点：数据量大，向redis写入数据压力大
        /*
        startupLogDStream.foreachRDD(rdd => {
            rdd.foreachPartition(it => {
                val client: Jedis = RedisUtil.getJedisClient
                it.foreach(log => {
                    // 存入到 Redis value 类型 set, 存储 uid
                    val key: String = "dau:" + log.logDate
                    client.sadd(key, log.uid)
                })
                client.close()
            })
        })
        */

        // 4.使用redis清洗和过滤数据：
        // 方案：对已经启动过的用户, 不需要再次向 Redis 写入这样的用户, 所以我们可以提前做过滤

        // 4.1 写入之前先过滤：使用redis(set)过滤，把启动过的设备uid存储到redis中, 然后用这个set去过滤流
        val filteredStartupLogDStream: DStream[StartupLog] = startupLogDStream.transform(rdd => {
            // 因为redis的数据不断的更新，所以不能在外部获取redis数据(程序启动后只执行一次)，必须针对每个批次都查询redis数据
            val client: Jedis = RedisUtil.getJedisClient
            // 获取redis中当天已存在的uid集合
            val uidSet: util.Set[String] = client.smembers(GmallConstant.REDIS_DAU_KEY + ":" + new SimpleDateFormat("yyyy-MM-dd").format(new Date()))
            client.close()

            // 广播变量：将uidSet广播到每个executor
            val uidSetBD: Broadcast[util.Set[String]] = ssc.sparkContext.broadcast(uidSet)

            // 根据redis中已存在的uid集合过滤出当前rdd中未写入过redis的uid
            val filteredRDD: RDD[StartupLog] = rdd.filter(log => !uidSetBD.value.contains(log.uid))

            // 4.2 批次内去重:  如果一个批次内, 一个设备多次启动(对这个设备来说是第一个批次), 则前面的没有完成去重(只过滤了之前批次已经写入redis的数据)
            // 考虑到一个窗口内, 右可能一个mid会启动多次, 所以需要做排序, 然后只取一个
            filteredRDD.map(log => (log.uid, log))
                .groupByKey()
                //.map(kv => kv._2.toList.sortBy(_.ts).take(1)) // 写法一
                .map {
                //case (uid, logIt) => logIt.toList.sortBy(_.ts).head // 写法二
                case (uid, logIt) => logIt.toList.minBy(_.ts) // 写法三：minBy = sortBy(_.ts).head
            }
        })
        filteredStartupLogDStream.print(1000)

        // 4.3 把第一次启动的数据写入到redis
        filteredStartupLogDStream.foreachRDD(rdd => {
            rdd.foreachPartition(startupLogIt => {
                val client: Jedis = RedisUtil.getJedisClient
                startupLogIt.foreach(startupLog => {
                    client.sadd(GmallConstant.REDIS_DAU_KEY + ":" + startupLog.logDate, startupLog.uid)
                })
                client.close()
            })
        })

        // 5. 写入hbase(借助phoenixphoenix)
        import org.apache.phoenix.spark._
        filteredStartupLogDStream.foreachRDD(rdd => {
            // 参数1: 表名  参数2: 列名组成的 seq 参数 zkUrl: zookeeper 地址
            rdd.saveToPhoenix("GMALL_DAU",
                Seq("MID", "UID", "APPID", "AREA", "OS", "CHANNEL", "LOGTYPE", "VERSION", "TS", "LOGDATE", "LOGHOUR"),
                zkUrl = Option("hadoop102,hadoop103,hadoop104:2181")
            )
        })

        ssc.start()
        ssc.awaitTermination()
    }
}