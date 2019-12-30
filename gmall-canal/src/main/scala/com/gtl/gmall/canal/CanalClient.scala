package com.gtl.gmall.canal

import java.net.InetSocketAddress

import com.alibaba.otter.canal.client.{CanalConnector, CanalConnectors}
import com.alibaba.otter.canal.protocol.CanalEntry.{EntryType, RowChange}
import com.alibaba.otter.canal.protocol.{CanalEntry, Message}
import com.google.protobuf.ByteString
import com.gtl.gmall.canal.util.CanalHandler

/*
1. 从canal读数据
    (1)先创建一个客户端对象
    (2)订阅数据
    (3)对数据解析解析

①message
    一次可以拉一个message, 一个message可以看成是由多条sql执行导致的变化组成的数据的封装
②Entry  实体
    一个message封装多个Entry. 一个Entry表示一条sql执行的结果(多行变化)
③StoreValue
    一个Entry封装一个StoreValue, 可以看成是序列化的数据
④RowChange
    表示一行变化的数据. 一个StoreValue会存储1个RowChange
⑤RowData
    一个RowChange包含了多个RowData，RowData表示一条变化的数据
⑥Column 列   列名和值

2. 把数据进行格式调整之后, 写入到kafka
 */
object CanalClient {
    def main(args: Array[String]): Unit = {
        // 1. 创建能连接到canal服务器的连接器对象
        val connector: CanalConnector = CanalConnectors.newSingleConnector(new InetSocketAddress("hadoop102", 11111), "example", "", "")
        // 2. 连接到canal服务器
        connector.connect()
        // 3. 监控指定的表的数据的变化
        connector.subscribe("gmall_online.order_info")
        // 4. 从canal中不断拉取数据
        while (true){
            // 5. 批次获取消息 (一个Message对应 多条sql 语句的执行)
            val msg: Message = connector.get(100) // 一次最多获取100条sql的执行
            println(msg.getId)
            // 6. 一个Message包含多个Entry，每个Entry对应一个sql语句的执行
            val entries: java.util.List[CanalEntry.Entry] = msg.getEntries
            import scala.collection.JavaConversions._
            if (entries != null && entries.nonEmpty) {
                // 7. 遍历每个Entry，即每个sql语句的执行后的变化
                for (entry <- entries){
                    // 8. EntryType.ROWDATA 只对这样的 EntryType 做处理
                    if (entry.getEntryType == EntryType.ROWDATA) {
                        // 9. 一个Entry封装一个StoreValue，获取到这行数据, 但是这种数据不是字符串, 所以要解析
                        val value: ByteString = entry.getStoreValue
                        // 10. 一个StroeValue存储了一个RowChange，将字节串解析为字符串
                        val rowChange: RowChange = RowChange.parseFrom(value)
                        // 11. 定义专门处理的工具类: 一个RowChange包含了多个RowData，RowData表示一条变化的数据
                        // 参数 1 表名, 参数 2 事件类型(插入, 删除等), 参数 3: 具体的数据
                        CanalHandler.handle(entry.getHeader.getTableName, rowChange.getEventType, rowChange.getRowDatasList)
                    }
                }
            } else {
                println("没有抓取到数据...., 2s 之后重新抓取")
                Thread.sleep(2000)
            }
        }
    }


}
