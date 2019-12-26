package com.gtl.gmall.realtime.util

import java.io.InputStream
import java.util.Properties

import scala.collection.mutable

object PropertiesUtil {

    // 方式一：缺点(只能读取指定文件的配置项)
    /*
    val properties: Properties = new Properties
    val inputStream: InputStream = ClassLoader.getSystemResourceAsStream("config.properties")
    properties.load(inputStream)

    def getProperty(propertyName: String): String = {
        properties.getProperty(propertyName)
    }
    */

    // 方式二：优化，可以读取不同配置文件的配置项
    // 每个配置文件对应一个Properties
    val propertiesMap: mutable.Map[String, Properties] = mutable.Map[String, Properties]()

    def getProperty(configFile: String, propertyName: String): String = {
        // 缺点：该形式会导致每次查询配置项时，都会重新获取输入流
        /*
        val properties: Properties = new Properties
        val inputStream: InputStream = ClassLoader.getSystemResourceAsStream(configFile)
        properties.load(inputStream)
        */

        // 优化：已存在的配置文件的Properties，再次读取配置项时可以直接从map中获取
        // 如果是第一次读取某个配置文件，则将该配置文件及其Properties保存在map中
        val properties: Properties = propertiesMap.getOrElseUpdate(configFile, {
            val properties: Properties = new Properties
            val inputStream: InputStream = ClassLoader.getSystemResourceAsStream(configFile)
            properties.load(inputStream)
            properties
        })

        properties.getProperty(propertyName)
    }

    def main(args: Array[String]): Unit = {
        println(getProperty("config.properties", "kafka.broker.list"))
        println(getProperty("config.properties", "kafka.broker.list"))
    }
}