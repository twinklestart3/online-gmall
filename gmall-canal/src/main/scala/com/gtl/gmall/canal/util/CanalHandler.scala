package com.gtl.gmall.canal.util

import java.util

import com.alibaba.fastjson.JSONObject
import com.alibaba.otter.canal.protocol.CanalEntry
import com.alibaba.otter.canal.protocol.CanalEntry.EventType
import com.gtl.gmall.common.constant.GmallConstant

object CanalHandler {
    /**
      * 处理从 canal 取来的数据
      *
      * @param tableName   表名
      * @param eventType   事件类型
      * @param rowDataList 数据类别
      */
    def handle(tableName: String, eventType: CanalEntry.EventType, rowDataList: util.List[CanalEntry.RowData]) = {
        import scala.collection.JavaConversions._

        if ("order_info" == tableName && eventType == EventType.INSERT && rowDataList.nonEmpty){
            // 1. rowData 表示一行数据, 通过他得到每一列. 首先遍历每一行数据
            for (rowData <- rowDataList){
                val obj: JSONObject = new JSONObject()
                // 2. 得到每行中, 所有列组成的列表
                val afterColumnsList: util.List[CanalEntry.Column] = rowData.getAfterColumnsList

                for (column <- afterColumnsList){
                    println(column.getName + ":" + column.getValue)
                    // 3. 得到列名和列值
                    obj.put(column.getName, column.getValue)
                }
                // 4. 发送到 Kafka
                MyKafkaSender.sendToKafka(GmallConstant.TOPIC_ORDER, obj.toJSONString)
            }
        }
    }

}
