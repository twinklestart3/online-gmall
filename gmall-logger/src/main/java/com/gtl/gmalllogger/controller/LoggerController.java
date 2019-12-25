package com.gtl.gmalllogger.controller;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.gtl.gmall.common.constant.GmallConstant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

/**
 * 业务:
 *
 * 1. 给日志添加时间戳 (客户端的时间有可能不准, 所以使用服务器端的时间)
 *
 * 2. 日志落盘
 *
 * 3. 日志发送 kafka
 */
@RestController
public class LoggerController {
    // 初始化Logger对象
    private final Logger logger = LoggerFactory.getLogger(LoggerController.class);

    // 使用注入的方式来实例化 KafkaTemplate ， Spring boot会自动完成
    @Autowired
    KafkaTemplate<String, String> kafka;

    @PostMapping("/log")
    public String doLog(@RequestParam("log") String log){
        //System.out.println(log);

        JSONObject logObj = JSON.parseObject(log);

        // 1.添加时间戳
        logObj = addTS(logObj);

        // 2.使用log4j给日志落盘
        saveLog2File(logObj);

        // 3.发送日志到kafka
        sendLogToKafka(logObj);

        return "success";
    }

    /**
     * 添加时间戳
     * @param logObj
     * @return
     */
    private JSONObject addTS(JSONObject logObj){
        logObj.put("ts", System.currentTimeMillis());
        return logObj;
    }

    /**
     * 日志落盘 使用log4j
     * @param logObj
     */
    private void saveLog2File(JSONObject logObj){
        logger.info(logObj.toJSONString());
    }

    /**
     * 发送日志到kafka
     * @param logObj
     */
    private void sendLogToKafka(JSONObject logObj){
        String logType = logObj.getString("logType");
        if ("startup".equals(logType)){
            kafka.send(GmallConstant.TOPIC_STARTUP, logObj.toJSONString());
        }else if("event".equals(logType)){
            kafka.send(GmallConstant.TOPIC_EVENT, logObj.toJSONString());
        }
    }
}
