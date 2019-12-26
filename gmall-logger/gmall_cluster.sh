#!/bin/bash
case $1 in
    "start")
    {
        for i in hadoop102 hadoop103 hadoop104
        do
            echo "========启动日志服务: $i==============="
            ssh $i  "source /etc/profile ;nohup java -jar /opt/online-gmall/gmall-logger-0.0.1-SNAPSHOT.jar >/dev/null 2>&1  &"
        done
     };;
    "stop")
    {
        for i in hadoop102 hadoop103 hadoop104
        do
            echo "========关闭日志服务: $i==============="
            ssh $i "ps -ef|grep gmall-logger-0.0.1-SNAPSHOT.jar | grep -v grep|awk '{print \$2}'|xargs kill"
        done
    };;

    *)
    {
        echo 启动姿势不对, 请使用参数 start 启动日志服务, 使用参数 stop 停止服务
    };;
esac